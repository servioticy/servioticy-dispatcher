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
import com.servioticy.dispatcher.bolts.*;
import com.servioticy.queueclient.KestrelThriftClient;
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

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        String path = null;
        if (cmd.hasOption("f")) {
            path = cmd.getOptionValue("f");
        }
        DispatcherContext dc = new DispatcherContext();
        dc.loadConf(path);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(dc.kestrelAddresses), dc.kestrelPort, "services", new UpdateDescriptorScheme()), 4);

        builder.setBolt("checkopid", new CheckOpidBolt(dc), 2)
                .shuffleGrouping("dispatcher");
        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(dc), 2)
                .shuffleGrouping("checkopid", "subscription");

        builder.setBolt("httpdispatcher", new HttpSubsDispatcherBolt(), 4)
                .fieldsGrouping("subretriever", "httpSub", new Fields("subid"));

        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(dc), 4)
                .shuffleGrouping("subretriever", "internalSub")
                .shuffleGrouping("checkopid", "stream");
        builder.setBolt("streamprocessor", new StreamProcessorBolt(dc), 4)
                .fieldsGrouping("streamdispatcher", new Fields("soid", "streamid"));


        Config conf = new Config();
        conf.setDebug(false);
        if (cmd.hasOption("t")) {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology(cmd.getOptionValue("t"), conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("dispatcher", conf, builder.createTopology());

        }

    }

}
