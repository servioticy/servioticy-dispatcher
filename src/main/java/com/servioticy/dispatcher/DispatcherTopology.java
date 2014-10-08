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
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.servioticy.datamodel.reputation.Reputation;
import com.servioticy.dispatcher.bolts.*;
import org.apache.commons.cli.*;

import java.util.Arrays;

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
        builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueue, new UpdateDescriptorScheme()), 8);
        builder.setSpout("actions", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, dc.kestrelQueueActions, new ActuationScheme()), 4);

        builder.setBolt("checkopid", new PrepareBolt(dc), 10)
                .shuffleGrouping("dispatcher");

        builder.setBolt("actuationdispatcher", new ActuationDispatcherBolt(dc), 2)
        		.shuffleGrouping("actions");

        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc), 4)
                .shuffleGrouping("checkopid", "subscription");

        builder.setBolt("httpdispatcher", new HttpSubsDispatcherBolt(), 1)
                .fieldsGrouping("subretriever", "httpSub", new Fields("subid"));
        builder.setBolt("pubsubdispatcher", new PubSubDispatcherBolt(dc), 1)
                .fieldsGrouping("subretriever", "pubsubSub", new Fields("subid"));

        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc), 13)
                .shuffleGrouping("subretriever", "internalSub")
                .shuffleGrouping("checkopid", "stream");
        builder.setBolt("streamprocessor", new StreamProcessorBolt(dc), 17)
                .shuffleGrouping("streamdispatcher", "default");

        builder.setBolt("reputation", new ReputationBolt(dc), 2)
                .shuffleGrouping("streamdispatcher", Reputation.STREAM_WO_SO)
                .shuffleGrouping("streamprocessor", Reputation.STREAM_SO_SO)
                .shuffleGrouping("pubsubdispatcher", Reputation.STREAM_SO_PUBSUB);

        if (dc.benchmark) {
            builder.setBolt("benchmark", new BenchmarkBolt(dc), 4)
                    .shuffleGrouping("streamdispatcher", "benchmark")
                    .shuffleGrouping("subretriever", "benchmark")
                    .shuffleGrouping("streamprocessor", "benchmark")
                    .shuffleGrouping("checkopid", "benchmark");
        }

        Config conf = new Config();
        conf.setDebug(cmd.hasOption("d"));
        if (cmd.hasOption("t")) {
            conf.setNumWorkers(8);
            StormSubmitter.submitTopology(cmd.getOptionValue("t"), conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(4);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("dispatcher", conf, builder.createTopology());

        }

    }

}
