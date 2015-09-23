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
package com.servioticy.dispatcher.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SOGroup;
import com.servioticy.datamodel.subscription.SOSubscription;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SOProcessor;
import com.servioticy.dispatcher.SOProcessor010;
import com.servioticy.restclient.FutureRestResponse;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestResponse;

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 */
public class StreamDispatcherBolt implements IRichBolt {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private TopologyContext context;
    private RestClient restClient;
    private DispatcherContext dc;
    private ObjectMapper mapper;


    public StreamDispatcherBolt(DispatcherContext dc){
        this.dc = dc;
    }

    // For testing purposes
    public StreamDispatcherBolt(DispatcherContext dc, RestClient restClient){
        this.dc = dc;
        this.restClient = restClient;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.mapper = new ObjectMapper();
        this.collector = collector;
        this.context = context;
        if(restClient == null){
            restClient = new RestClient();
        }
    }

    public void execute(Tuple input) {
        SOSubscription soSub = null;
        SO so;
        RestResponse rr;
        FutureRestResponse frr;

        String suDoc = input.getStringByField("su");
        String docId = input.getStringByField("docid");
        String destination = input.getStringByField("destination");

        String soDoc;

        try{
            frr = restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + destination, null, RestClient.GET,
                    null);
            rr = frr.get();
            soDoc = rr.getResponse();
            so = this.mapper.readValue(soDoc,
                    SO.class);

            if(input.getSourceStreamId().equals("stream")){
                if(so.getGroups() != null){
                    for(Entry<String, SOGroup> group: so.getGroups().entrySet()){
                        // If there is a group called like the stream, then the docname refers to the group.
                        if(group.getKey().equals(docId)){
                            collector.ack(input);
                            return;
                        }
                    }
                }
            }

            SOProcessor sop = SOProcessor.factory(so, this.mapper);
            if(sop.getClass() == SOProcessor010.class) {
                soDoc = ((SOProcessor010)sop).replaceAliases();
                ((SOProcessor010)sop).compileJSONPaths();
            }

            SensorUpdate su = this.mapper.readValue(suDoc, SensorUpdate.class);
            boolean emitted = false;
            for (String streamIdByDoc : sop.getStreamsBySourceId(docId)) {
                // If the SU comes from the same stream than it is going, it must be stopped
//                boolean beenThere = false;
//                for (ArrayList<String> prevStream : su.getTriggerPath()) {
//                    beenThere = (destination == prevStream.get(0) && streamIdByDoc == prevStream.get(1));
//                    if (beenThere) break;
//                }
//
//                if (beenThere) {
//                    continue;
//                }
                this.collector.emit("default", input,
                        new Values(destination,
                                streamIdByDoc,
                                soDoc,
                                docId,
                                suDoc));
                emitted = true;
            }
            if (!emitted) {
                PathPerformanceBolt.send(collector, input, dc, suDoc, "no-stream");
            }
        } catch(RestClientErrorCodeException e){
            // TODO Log the error
            e.printStackTrace();
            if(e.getRestResponse().getHttpCode()>= 500){
                collector.fail(input);
                return;
            }
            PathPerformanceBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        }catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            PathPerformanceBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        }
        collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("default", new Fields("soid", "streamid", "so", "originid", "su"));
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}