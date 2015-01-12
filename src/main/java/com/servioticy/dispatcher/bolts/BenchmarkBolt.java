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
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;

import java.io.File;
import java.io.FileWriter;
import java.util.Map;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class BenchmarkBolt implements IRichBolt {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private TopologyContext context;
    private DispatcherContext dc;
    private ObjectMapper mapper;

    public BenchmarkBolt(DispatcherContext dc) {
        this.dc = dc;
    }

    public static void send(OutputCollector collector, Tuple input, DispatcherContext dc, String suDoc, String reason){
        if (dc.benchmark) collector.emit("benchmark", input,
                new Values(suDoc,
                        System.currentTimeMillis(),
                        reason)
        );
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.context = topologyContext;
        this.mapper = new ObjectMapper();

    }

    @Override
    public void execute(Tuple input) {
        String suDoc = input.getStringByField("su");
        Long stopTS = input.getLongByField("stopts") == null ? System.currentTimeMillis() : input.getLongByField("stopts");
        String reason = input.getStringByField("reason") == null ? "timeout" : input.getStringByField("reason");

        SensorUpdate su;
        try {
            su = this.mapper.readValue(suDoc, SensorUpdate.class);

            int chainSize = su.getPathTimestamps() == null ? 0 : su.getPathTimestamps().size();

            String csvLine = Long.toHexString(su.getOriginId()) + "," + su.getLastUpdate() + "," + stopTS + "," + reason + "," + chainSize;
            for (int i = 0; i < chainSize; i++) {
                csvLine += ",";
                csvLine += su.getPathTimestamps().get(i) + ",";
                csvLine += su.getTriggerPath().get(i).get(0) + "," + su.getTriggerPath().get(i).get(1);
            }

            File file = new File(dc.benchResultsDir + "/" + context.getThisTaskId() + ".csv");
            file.createNewFile();
            FileWriter writer = new FileWriter(file, true);
            writer.append(csvLine + "\n");
            writer.flush();
            writer.close();
        } catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            collector.ack(input);
            return;
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
