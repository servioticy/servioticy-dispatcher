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
import backtype.storm.LocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.testing.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.servioticy.datamodel.*;
import com.servioticy.dispatcher.ActuationScheme;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.UpdateDescriptorScheme;
import com.servioticy.dispatcher.bolts.*;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestClientException;
import com.servioticy.restclient.RestResponse;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class TopologyTest {

    @Test
    public void testBasicSO{
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4); // TODO change this
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam,new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                DispatcherContext dc = new DispatcherContext();

                String opid = "someopid";

                Subscriptions subscriptions = mapper.readValue(new File("subscriptions-group.json"), Subscriptions.class);
                String subscriptionsStr = mapper.writeValueAsString(subscriptions);
                SO so = mapper.readValue(new File("so-basic.json"), SO.class);
                String soStr = mapper.writeValueAsString(so);

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
                                + "private/" + opid, null,
                        RestClient.GET,
                        null)).thenReturn(new RestResponse("", 200));
                // get subscriptions
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/A"
                                + "/subscriptions/", null, RestClient.GET,
                        null)).thenReturn(new RestResponse(subscriptionsStr, 200));
                // get so
                when(restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId(), null, RestClient.GET,
                        null)).thenReturn(new RestResponse(soStr, 200));

                TopologyBuilder builder = new TopologyBuilder();
                QueueClient qc = QueueClient.factory("queue-simple.xml");
                qc.connect();

                builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);
                builder.setSpout("actions", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueueActions, new ActuationScheme()), 4);

                builder.setBolt("checkopid", new CheckOpidBolt(dc), 10)
                        .shuffleGrouping("dispatcher");

                builder.setBolt("actuationdispatcher", new ActuationDispatcherBolt(dc), 2)
                        .shuffleGrouping("actions");

                builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc), 4)
                        .shuffleGrouping("checkopid", "subscription");

                builder.setBolt("pubsubdispatcher", new PubSubDispatcherBolt(dc), 1)
                        .fieldsGrouping("subretriever", "pubsubSub", new Fields("subid"));

                builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc), 13)
                        .shuffleGrouping("subretriever", "internalSub")
                        .shuffleGrouping("checkopid", "stream");
                builder.setBolt("streamprocessor", new StreamProcessorBolt(dc), 17)
                        .shuffleGrouping("streamdispatcher", "default");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("dispatcher", new Values(opid, so.getId(), "A", ));
                mockedSources.addMockData("actions");

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                // check whether the result is right
                assertTrue(Testing.multiseteq(new Values(new Values("nathan"),
                        new Values("bob"), new Values("joey"), new Values(
                        "nathan")), Testing.readTuples(result, "1")));
                assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1),
                        new Values("nathan", 2), new Values("bob", 1),
                        new Values("joey", 1)), Testing.readTuples(result, "2")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "3")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
                        new Values(3), new Values(4)), Testing.readTuples(
                        result, "4")));
            }


        }
        });

    }

	@Test
	public void testExampleTopology() {

		try{

			String soid = "1234567890";
			String origStreamid = "location";

			DispatcherContext dc = new DispatcherContext();
            dc.loadConf(null);

			ObjectMapper mapper = new ObjectMapper();

			// Subscriptions document
			String subscriptions =	"{'subscriptions': []}";
			// Subscriber SO
			String so =	"{\n" +
                        "    \"streams\": {\n" +
                        "        \"weather\": {\n" +
                        "            \"channels\":" +
                        "                \"temperature\": {\n" +
                        "                    \"type\": \"number\",\n" +
                        "                    \"unit\": \"degrees\"\n" +
                        "                }\n" +
                        "            }\n" +
                        "        },\n" +
                        "        \"fahrenheit\": {\n" +
                        "            \"channels\": {\n" +
                        "                \"f\": {\n" +
                        "                  \"current-value\": \"function(weather){return weather.channels.temperature.current-value * 1.8 + 32}\",\n" +
                        "                    \"type\": \"number\"\n" +
                        "                }\n" +
                        "            }\n" +
                        "        },\n" +
                        "        \"aboveSeventy\": {\n" +
                        "            \"channels\": {\n" +
                        "                \"temperature\": {\n" +
                        "                    \"current-value\": \"function(fahrenheit){return fahrenheit.channels.f.current-value}\",\n" +
                        "                    \"type\": \"number\"\n" +
                        "                }\n" +
                        "            },\n" +
                        "          \"post-filter\": \"function(result){result.channels.temperature.current-value > 70}\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "}";

			// group1 SU
			String weatherSU=	"{\n" +
                                "    \"channels\": {\n" +
                                "        \"temperature\":  {\n" +
                                "            \"current-value\": 80\n" +
                                "\t\t}\n" +
                                "    },\n" +
                                "    \"lastUpdate\": 1199192639\n" +
                                "}";

			// near SU
			String nearSU =		"{" +
									"\"channels\": {" +
										"\"n\":{" +
											"\"current-value\": true" +
										"}" +
									"}," +
									"\"lastUpdate\": 1392981636" +
								"}";
			// proximity SU
			String proxSU = 	"{" +
									"\"channels\": {" +
										"\"p\":{" +
											"\"current-value\": 0.3234" +
										"}" +
									"}," +
									"\"lastUpdate\": 1392981236" +
								"}";

			//  Group last update request
			SOGroup group = new SOGroup();
			group.setStream("location");
			ArrayList<String> soids = new ArrayList<String>();
			soids.add("origin4");
			soids.add("origin5");
			group.setSoIds(soids);

			// Mocking up the rest calls...
			RestClient restClient = mock(RestClient.class, withSettings().serializable());
			// get opid
			when(restClient.restRequest(
					dc.restBaseURL
					+ "private/" + opid, null,
					RestClient.GET,
					null)).thenReturn(new RestResponse("", 200));
			// get subscriptions
			when(restClient.restRequest(
					dc.restBaseURL
					+ "private/" + soid + "/streams/weather"
					+ "/subscriptions/", null, RestClient.GET,
					null)).thenReturn(new RestResponse(subscriptions, 200));
            when(restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + soid + "/streams/fahrenheit"
                            + "/subscriptions/", null, RestClient.GET,
                    null)).thenReturn(new RestResponse(subscriptions, 200));
            when(restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + soid + "/streams/aboveSeventy"
                            + "/subscriptions/", null, RestClient.GET,
                    null)).thenReturn(new RestResponse(subscriptions, 200));

			// get so
			when(restClient.restRequest(
					dc.restBaseURL
					+ "private/" + soid, null, RestClient.GET,
					null)).thenReturn(new RestResponse(so, 200));
			// store new SUs
			when(restClient.restRequest(
					any(String.class),
					any(String.class), eq(RestClient.PUT),
					any(Map.class))).thenReturn(new RestResponse("", 200));


			TopologyBuilder builder = new TopologyBuilder();
			QueueClient qc = QueueClient.factory("queue-simple.xml");
			qc.connect();


			FeederSpout feeder = new FeederSpout(new Fields("opid", "soid", "streamid", "su"));

			builder.setSpout("dispatcher", feeder);

	        builder.setBolt("checkopid", new CheckOpidBolt(dc, restClient), 1)
	        	.shuffleGrouping("dispatcher");
	        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc, restClient), 1)
        		.shuffleGrouping( "checkopid", "subscription");

	        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc, restClient), 1)
	    		.shuffleGrouping("subretriever", "internalSub")
	    		.shuffleGrouping("checkopid", "stream");
	        builder.setBolt("streamprocessor", new StreamProcessorBolt(dc, qc, restClient), 1)
				.fieldsGrouping("streamdispatcher", new Fields("soid", "streamid"));


	        Config conf = new Config();
	        conf.setDebug(true);

	    	LocalCluster cluster = new LocalCluster();
	    	cluster.submitTopology("dispatcher", conf, builder.createTopology());

	    	feeder.feed(new Values("sometestopid", "1234567890", "weather", weatherSU));

	    	String newDescriptor;
	    	while((newDescriptor = (String) qc.get()) == null){
	    		try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	}
	    	Assert.assertTrue("Return value", newDescriptor != null);

	    	UpdateDescriptor ud = mapper.readValue(newDescriptor, UpdateDescriptor.class);

	    	Assert.assertTrue("Operation id", ud.getOpid() != null);
	    	Assert.assertTrue("Origin SO id", ud.getSoid().equals(soid));
	    	Assert.assertTrue("Origin stream id", ud.getStreamid().equals("fahrenheit"));
	    	Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 1199192639);
	    	SUChannel such = ud.getSu().getChannels().get("f");
	    	double proximity = (Double)such.getCurrentValue();
	    	Assert.assertTrue("New SU current-value", proximity == 176);
		} catch (RestClientException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		} catch (RestClientErrorCodeException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		} catch (JsonParseException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		} catch (JsonMappingException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		} catch (IOException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		} catch (QueueClientException e) {
			fail("Test failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}
}
