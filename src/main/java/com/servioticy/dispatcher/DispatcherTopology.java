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

import java.util.Arrays;

import com.servioticy.dispatcher.bolts.CheckOpidBolt;
import com.servioticy.dispatcher.bolts.HttpSubsDispatcherBolt;
import com.servioticy.dispatcher.bolts.PubSubDispatcherBolt;
import com.servioticy.dispatcher.bolts.StreamDispatcherBolt;
import com.servioticy.dispatcher.bolts.StreamProcessorBolt;
import com.servioticy.dispatcher.bolts.SubscriptionRetrieveBolt;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class DispatcherTopology {
	
	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		// TODO Auto-generated method stub
		DispatcherContext.loadConf();

		TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("dispatcher", new KestrelThriftSpout(Arrays.asList(DispatcherContext.kestrelIPs), DispatcherContext.kestrelPort, "services", new UpdateDescriptorScheme()), 5);
        
        builder.setBolt("checkopid", new CheckOpidBolt(), 2)
        	.shuffleGrouping("dispatcher");
        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(), 2)
        	.shuffleGrouping( "checkopid", "subscription");
        
        builder.setBolt("httpdispatcher", new HttpSubsDispatcherBolt(), 4)
        	.fieldsGrouping("subretriever", "httpSub", new Fields("subid"));
        builder.setBolt("pubsubdispatcher", new PubSubDispatcherBolt(), 4)
    		.fieldsGrouping("subretriever", "pubsubSub", new Fields("subid"));
        
        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(), 4)
    		.shuffleGrouping("subretriever", "internalSub")
    		.shuffleGrouping("checkopid", "stream");
        builder.setBolt("streamprocessor", new StreamProcessorBolt(), 4)
			.fieldsGrouping("streamdispatcher", new Fields("soid", "streamid"));
        
        
        Config conf = new Config();
        conf.setDebug(true);
        if(args!=null && args.length > 0){
        	conf.setNumWorkers(6);
        	StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else{
        	conf.setMaxTaskParallelism(3);
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology("dispatcher", conf, builder.createTopology());

        }

	}

}
