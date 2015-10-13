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
package com.servioticy.dispatcher;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.servioticy.dispatcher.bolts.*;
import org.apache.commons.cli.*;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class DispatcherTopology {

    /**
     * @param args
     * @throws InvalidTopologyException
     * @throws AlreadyAliveException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, ParseException {
        Options options = new Options();
        
        options.addOption(OptionBuilder.withArgName("file")
                .hasArg()
                .withDescription("Config file path.")
                .create("f"));
        options.addOption(OptionBuilder.withArgName("topology")
                .hasArg()
                .withDescription("Name of the topology in storm. If no name is given it will run in local mode.")
                .create("t"));
        options.addOption(OptionBuilder
                .withDescription("Enable debugging")
                .create("d"));

        
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        String path = null;
        if (cmd.hasOption("f")) {
            path = cmd.getOptionValue("f");
        }

        DispatcherContext dc = new DispatcherContext();
        dc.loadConf(path);
                
        TopologyBuilder builder = new TopologyBuilder();

        // TODO Auto-assign workers to the spout in function of the number of Kestrel IPs
        String updatesBrokerZkStr = "";
        for(String updateAddress: dc.updatesAddresses){
            updatesBrokerZkStr += updateAddress + ",";
        }
        updatesBrokerZkStr = updatesBrokerZkStr.substring(0, updatesBrokerZkStr.length()-1);
        BrokerHosts updatesHosts = new ZkHosts(updatesBrokerZkStr);

        SpoutConfig updatesSpoutConfig = new SpoutConfig(updatesHosts, dc.updatesQueue, "/" + dc.updatesQueue, "dispatcher-" + dc.updatesQueue);

        updatesSpoutConfig.scheme = new SchemeAsMultiScheme(new UpdateDescriptorScheme());

        builder.setSpout("updates", new KafkaSpout(updatesSpoutConfig), 8);

        builder.setBolt("prepare", new PrepareBolt(dc), 48)
                .shuffleGrouping("updates");

        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc), 48)
                .shuffleGrouping("prepare", "subscription");
        
        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc), 48)
                .shuffleGrouping("subretriever", "streamSub")
                .shuffleGrouping("prepare", "stream");
        builder.setBolt("streamprocessor", new StreamProcessorBolt(dc), 48)
                .shuffleGrouping("streamdispatcher", "default");

        if (dc.benchmark) {
            builder.setBolt("benchmark", new PathPerformanceBolt(dc), 48)
                    .shuffleGrouping("streamdispatcher", "benchmark")
                    .shuffleGrouping("subretriever", "benchmark")
                    .shuffleGrouping("streamprocessor", "benchmark")
                    .shuffleGrouping("prepare", "benchmark");
            builder.setBolt("stages", new StagesPerformanceBolt(dc), 48)
                    .shuffleGrouping("streamprocessor", "stages")
                    .shuffleGrouping("prepare", "stages");
        }

        Config conf = new Config();
        conf.setDebug(cmd.hasOption("d"));
        if (cmd.hasOption("t")) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(cmd.getOptionValue("t"), conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(4);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("dispatcher", conf, builder.createTopology());

        }

    }

}
