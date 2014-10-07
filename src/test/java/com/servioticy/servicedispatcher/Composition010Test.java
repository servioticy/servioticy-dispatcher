/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.servioticy.servicedispatcher;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.testing.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import com.servioticy.datamodel.*;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SO010;
import com.servioticy.datamodel.sensorupdate.SUChannel;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.datamodel.subscription.Subscriptions;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.UpdateDescriptorScheme;
import com.servioticy.dispatcher.bolts.*;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 */
public class Composition010Test {

    @Test
    public void testBasicCompositionFromStream() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-basic.json").toURI()), SO.class);
                String soStr = mapper.writeValueAsString(so);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                String suAStr = mapper.writeValueAsString(suA);
                SensorUpdate suGroup = mapper.readValue(new File(cl.getResource("0.1.0/su-group.json").toURI()), SensorUpdate.class);
                String suGroupStr = mapper.writeValueAsString(suGroup);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/groups/lastUpdate", mapper.writeValueAsString(so.getGroups().get("group")),
                        RestClient.POST,
                        null)).thenReturn(new RestResponse(suGroupStr, 200));

                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", suAStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(),
                                "B",
                                soStr,
                                "A",
                                suAStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 50) {
                        Assert.fail("Timeout");
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                Assert.assertTrue("Return value", newDescriptor != null);

                UpdateDescriptor ud = mapper.readValue(newDescriptor, UpdateDescriptor.class);

                Assert.assertTrue("Operation id", ud.getOpid() != null);
                Assert.assertTrue("Origin SO id", ud.getSoid().equals(so.getId()));
                Assert.assertTrue("Origin stream id", ud.getStreamid().equals("B"));
                Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 2);
                SUChannel suCh = ud.getSu().getChannels().get("a");
                double cValue = (Double) suCh.getCurrentValue();
                Assert.assertTrue("New SU current-value", cValue == 2);
                qc.disconnect();
            }
        });
    }

    @Test
    public void testBasicCompositionFromGroup() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                Subscriptions subscriptions = mapper.readValue(new File(cl.getResource("0.1.0/subscriptions-group.json").toURI()), Subscriptions.class);
                String subscriptionsStr = mapper.writeValueAsString(subscriptions);
                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-basic.json").toURI()), SO.class);
                String soStr = mapper.writeValueAsString(so);
                SO additionalSO = mapper.readValue(new File(cl.getResource("0.1.0/additional_so.json").toURI()), SO.class);
                String additionalSOStr = mapper.writeValueAsString(additionalSO);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                String suAStr = mapper.writeValueAsString(suA);
                SensorUpdate suGroup = mapper.readValue(new File(cl.getResource("0.1.0/su-group.json").toURI()), SensorUpdate.class);
                String suGroupStr = mapper.writeValueAsString(suGroup);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/additional_so/streams/some_stream"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(subscriptionsStr, 200));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/additional_so", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(additionalSOStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(suAStr, 200));

                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, "additional_so", "some_stream", suGroupStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, "additional_so", "some_stream", suGroupStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, "additional_so", "some_stream", suGroupStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values("additional_so", "some_stream", suGroupStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(mapper.writeValueAsString(subscriptions.getSubscriptions().get(0)),
                                suGroupStr,
                                "additional_so", "some_stream")),
                        Testing.readTuples(result, "subretriever", "internalSub")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values("so",
                                "B",
                                soStr,
                                "group",
                                suGroupStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 10) {
                        Assert.fail("Timeout");
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                Assert.assertTrue("Return value", newDescriptor != null);

                UpdateDescriptor ud = mapper.readValue(newDescriptor, UpdateDescriptor.class);

                Assert.assertTrue("Operation id", ud.getOpid() != null);
                Assert.assertTrue("Origin SO id", ud.getSoid().equals(so.getId()));
                Assert.assertTrue("Origin stream id", ud.getStreamid().equals("B"));
                Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 2);
                SUChannel suCh = ud.getSu().getChannels().get("a");
                double cValue = (Double) suCh.getCurrentValue();
                Assert.assertTrue("New SU current-value", cValue == 2);
                qc.disconnect();
            }
        });
    }

    @Test
    public void testSelfLastUpdate() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-self.json").toURI()), SO.class);
                String soStr = mapper.writeValueAsString(so);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                String suAStr = mapper.writeValueAsString(suA);
                SensorUpdate suB = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                String suBStr = mapper.writeValueAsString(suB);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(suBStr, 200));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", suAStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(),
                                "B",
                                soStr,
                                "A",
                                suAStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 50) {
                        Assert.fail("Timeout");
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                Assert.assertTrue("Return value", newDescriptor != null);

                UpdateDescriptor ud = mapper.readValue(newDescriptor, UpdateDescriptor.class);

                Assert.assertTrue("Operation id", ud.getOpid() != null);
                Assert.assertTrue("Origin SO id", ud.getSoid().equals(so.getId()));
                Assert.assertTrue("Origin stream id", ud.getStreamid().equals("B"));
                Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 2);
                SUChannel suCh = ud.getSu().getChannels().get("a");
                double cValue = (Double) suCh.getCurrentValue();
                Assert.assertTrue("New SU current-value", cValue == 2);
                qc.disconnect();
            }
        });
    }

    @Test
    public void testEmptyLastUpdate() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-lunull.json").toURI()), SO.class);
                String soStr = mapper.writeValueAsString(so);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                String suAStr = mapper.writeValueAsString(suA);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", suAStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(),
                                "B",
                                soStr,
                                "A",
                                suAStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 50) {
                        Assert.fail("Timeout");
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                Assert.assertTrue("Return value", newDescriptor != null);

                UpdateDescriptor ud = mapper.readValue(newDescriptor, UpdateDescriptor.class);

                Assert.assertTrue("Operation id", ud.getOpid() != null);
                Assert.assertTrue("Origin SO id", ud.getSoid().equals(so.getId()));
                Assert.assertTrue("Origin stream id", ud.getStreamid().equals("B"));
                Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 2);
                SUChannel suCh = ud.getSu().getChannels().get("a");
                boolean cValue = (Boolean) suCh.getCurrentValue();
                Assert.assertTrue("New SU current-value", cValue);
                qc.disconnect();
            }
        });
    }

    @Test
    public void testPreFilter() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-prefilter.json").toURI()), SO.class);
                String soStr = mapper.writeValueAsString(so);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                suA.getChannels().get("a").setCurrentValue(-1);
                String suAStr = mapper.writeValueAsString(suA);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", suAStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(),
                                "B",
                                soStr,
                                "A",
                                suAStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 40) {
                        Assert.assertTrue("Filtered", true);
                        return;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                qc.disconnect();
                Assert.fail("Not filtered");
            }
        });
    }

    @Test
    public void testPostFilter() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                ClassLoader cl = Thread.currentThread().getContextClassLoader();

                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();
                dc.loadConf(null);

                String opid = "someopid";

                SO so = mapper.readValue(new File(cl.getResource("0.1.0/so-postfilter.json").toURI()), SO010.class);
                String soStr = mapper.writeValueAsString(so);
                SensorUpdate suA = mapper.readValue(new File(cl.getResource("0.1.0/su-A.json").toURI()), SensorUpdate.class);
                suA.setLastUpdate(2);
                suA.getChannels().get("a").setCurrentValue(-1);
                String suAStr = mapper.writeValueAsString(suA);

                // Mocking up the rest calls...
                RestClient restClient = mock(RestClient.class, withSettings().serializable());
                // store new SUs
                when(restClient.restRequest(
                        any(String.class),
                        any(String.class), eq(RestClient.PUT),
                        any(Map.class))).thenReturn(new RestResponse("", 200));
                // get opid
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));
                // get SU
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/B/lastUpdate",
                        null, RestClient.GET,
                        null)).thenReturn(new RestResponse(null, 204));

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);

                builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", suAStr));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                Assert.assertTrue(Testing.multiseteq(new Values(new Values(opid, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "dispatcher", "default")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(null, so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "stream")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(), "A", suAStr)),
                        Testing.readTuples(result, "checkopid", "subscription")));

                Assert.assertTrue(Testing.multiseteq(new Values(new Values(so.getId(),
                                "B",
                                soStr,
                                "A",
                                suAStr)),
                        Testing.readTuples(result, "streamdispatcher", "default")));

                String newDescriptor;
                int i = 0;
                while ((newDescriptor = (String) qc.get()) == null) {
                    if (i == 50) {
                        Assert.assertTrue("Filtered", true);
                        return;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    i++;
                }
                qc.disconnect();
                Assert.fail("Not filtered");
            }
        });
    }
}
