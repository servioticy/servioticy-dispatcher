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
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 */
public class PrepareBolt implements IRichBolt {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private RestClient restClient;
    private DispatcherContext dc;

    public PrepareBolt(DispatcherContext dc) {
        this.dc = dc;
    }

    // For testing purposes
    public PrepareBolt(DispatcherContext dc, RestClient restClient) {
        this.dc = dc;
        this.restClient = restClient;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        if (restClient == null) {
            restClient = new RestClient();
        }

    }

    public void execute(Tuple input) {


        RestResponse rr;
        String opid = input.getStringByField("opid");
        String suDoc = input.getStringByField("su");
        String soid = input.getStringByField("soid");
        String streamid = input.getStringByField("streamid");
        ObjectMapper mapper = new ObjectMapper();
        SensorUpdate su;
        try {
                su = mapper.readValue(suDoc, SensorUpdate.class);


            try {
                rr = restClient.restRequest(
                        dc.restBaseURL
                                + "private/opid/" + opid, null,
                        RestClient.GET, null
                );

            } catch (Exception e) {
                // TODO Log the error
                // Retry until timeout
                this.collector.fail(input);
                return;
            }

            // Benchmark
            if(dc.benchmark) {

                if (su.getTriggerPath() == null) {
                    su.setTriggerPath(new ArrayList<ArrayList<String>>());
                    String[] chainInit = {soid, streamid};
                    su.getTriggerPath().add(new ArrayList<String>(Arrays.asList(chainInit)));
                    su.setPathTimestamps(new ArrayList<Long>());
                    su.getPathTimestamps().add(System.currentTimeMillis());
                    su.setOriginId(UUID.randomUUID().getMostSignificantBits());
                }
                /*else if( (System.currentTimeMillis() - su.getPathTimestamps().get(su.getPathTimestamps().size()-1)) > 2*60*1000 ||
                         (System.currentTimeMillis() - su.getPathTimestamps().get(0)) > 10*60*1000){
                    // Timeout
                    this.collector.emit("benchmark", input,
                            new Values(suDoc,
                                    System.currentTimeMillis(),
                                    "timeout")
                    );
                    collector.ack(input);
                    //TODO Log the error
                    return;
                }*/

                suDoc = mapper.writeValueAsString(su);
            }
        } catch (Exception e) {
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            // TODO Log the error
            e.printStackTrace();
            collector.ack(input);
            return;
        }
        this.collector.emit(
                "stream",
                input,
                new Values(streamid,
                        soid,
                        suDoc)
        );

        this.collector.emit(
                "subscription",
                input,
                new Values(soid,
                        streamid,
                        suDoc)
        );
        this.collector.ack(input);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("subscription", new Fields("soid", "streamid", "su"));
        declarer.declareStream("stream", new Fields("docid", "destination", "su"));
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}