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
import com.servioticy.datamodel.sensorupdate.ProvenanceUnit;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.restclient.FutureRestResponse;
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
    private ObjectMapper mapper;


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
        this.mapper = new ObjectMapper();

        if (restClient == null) {
            restClient = new RestClient();
        }

    }

    public static SensorUpdate initializeProvenance(SensorUpdate su, String soid, String streamid){
        if(su.getProvenance() == null){
            su.setProvenance(new ArrayList<ArrayList<ProvenanceUnit>>());
        }
        if(su.getProvenance().isEmpty()){
            su.getProvenance().add(new ArrayList<ProvenanceUnit>());
        }
        if(su.getProvenance().get(0) == null){
            su.getProvenance().set(0, new ArrayList<ProvenanceUnit>());
        }
        if(su.getProvenance().get(0).isEmpty()){
            su.getProvenance().get(0).add(new ProvenanceUnit(soid, streamid, System.currentTimeMillis()));

        }
        if(su.getProvenance().get(0).get(0) == null){
            su.getProvenance().get(0).set(0, new ProvenanceUnit(soid, streamid, System.currentTimeMillis()));
        }
        return su;
    }

    public void execute(Tuple input) {
        RestResponse rr;
        FutureRestResponse frr;
        String suDoc = input.getStringByField("su");
        String soid = input.getStringByField("soid");
        String streamid = input.getStringByField("streamid");
        SensorUpdate su;
        try {
            su = this.mapper.readValue(suDoc, SensorUpdate.class);

            // Benchmark
            if(dc.benchmark) {
                if (su.getOriginId() == null){
                    su.setOriginId(UUID.randomUUID().getMostSignificantBits());
                }
                suDoc = this.mapper.writeValueAsString(su);
            }
            // In case the updates does not have provenance, it is added here.
            // Transitional
            suDoc = this.mapper.writeValueAsString(initializeProvenance(su, soid, streamid));

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
                        suDoc,
                        soid)
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
        declarer.declareStream("stream", new Fields("docid", "destination", "su", "originso"));
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}