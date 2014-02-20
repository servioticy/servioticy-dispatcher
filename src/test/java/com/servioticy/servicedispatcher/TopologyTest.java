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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.servioticy.datamodel.SO;
import com.servioticy.datamodel.SOSubscription;
import com.servioticy.datamodel.Subscription;
import com.servioticy.datamodel.Subscriptions;
import com.servioticy.dispatcher.SDispatcherContext;
import com.servioticy.dispatcher.bolts.CheckOpidBolt;
import com.servioticy.dispatcher.bolts.HttpSubsDispatcherBolt;
import com.servioticy.dispatcher.bolts.PubSubDispatcherBolt;
import com.servioticy.dispatcher.bolts.StreamDispatcherBolt;
import com.servioticy.dispatcher.bolts.StreamProcessorBolt;
import com.servioticy.dispatcher.bolts.SubscriptionRetrieveBolt;
import com.servioticy.queueclient.QueueClient;

import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestResponse;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.FixedTupleSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class TopologyTest {
	
	@Test
	public void testExampleTopology() {
//		try {
//			
//		} catch (QueueClientException e) {
//			// TODO Auto-generated catch block
//			fail("Factory failed: " + e.getMessage() + "\n" + e.getStackTrace());
//		}
		String opid = "sometestopid";
		String originSoid = "1234567890";
		String origStreamid = "location";
		String subId = "0";
		String destSoid1 = "2345678901";
		String destStreamid1 = "near";
		
		ObjectMapper mapper = new ObjectMapper();
		
		// Subscriptions document
		String subscriptions =	"{" +
									"\"subscriptions\": [" +
										"{" +
											"\"callback\": \"internal\"," +
										    "\"destination\": \"" + destSoid1 + "\"," +
										    "\"customFields\": {" + 
										        "\"groupId\": \"group1\"" + 
										    "}" + 
									    "}" +
									 "]" +
								"}";
		// Subscriber SO
		String so =				"{" +
									"\"aliases\":[" +
										"{\"@nearDistance@\": \"0.0001\"}," +
										"{\"@latGroup1@\": \"\"}," +
										"{\"@latGroup2@\": \"\"}," +
										"{\"@longGroup1@\": \"\"}," +
										"{\"@longGroup2@\": \"\"}," +
										"{\"@latDistance@\": \"\"}," +
										"{\"@longDistance@\": \"\"}," +
										"{\"@distance@\": \"\"}" +
									"]," +
									"\"groups\":{" +
										"\"group1\":{" +
											"\"members\":[" +
												"\"origin2\"," +
												"\"origin3\"" +
											"]," +
											"\"stream\": \"location\"" +
										"}," +
										"\"group2\":{" +
											"\"members\":[" +
												"\"origin3\"," +
												"\"origin4\"" +
											"]," +
											"\"stream\": \"location\"" +
										"}" +
									"}," +
									"\"streams\":{" +
										"\"proximity\":{" +
											
										"}," +
										"\"near\":{" +
										
										"}" +
									"}" +
								"}";
		
		// Mocking up the rest calls...
		RestClient restClient = mock(RestClient.class);
		// get opid
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ opid, null,
				RestClient.GET,
				null)).thenReturn(new RestResponse("", 200));
		// get subscriptions
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ originSoid + "/streams/"
				+ origStreamid
				+ "/subscriptions/", null, RestClient.GET,
				null)).thenReturn(new RestResponse(mapper.writeValueAsString(subscriptions), 200));
		// get subscriber so
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ destSoid1, null, RestClient.GET,
				null)).thenReturn(new RestResponse("", 200));
		// get 'group2' location group last update
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ destSoid2, null, RestClient.POST,
				null)).thenReturn(new RestResponse("", 200));
		// get 'near' stream last update
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ destSoid2, null, RestClient.GET,
				null)).thenReturn(new RestResponse("", 200));
		// store new SU for subscriber so
		when(restClient.restRequest(
				SDispatcherContext.restBaseURL
				+ destSoid2 + "/streams/"
				+ streamId + "/store.data", apiJson,
				RestClient.PUT,
				 null)).thenReturn(new RestResponse("", 200));
		
		
		SDispatcherContext.loadConf();

		TopologyBuilder builder = new TopologyBuilder();
		QueueClient qc = QueueClient.factory("queue-simple.xml");

        
		FeederSpout feeder = new FeederSpout(new Fields("opid", "streamid", "su"));
		
		builder.setSpout("dispatcher", feeder);
		
        builder.setBolt("checkopid", new CheckOpidBolt(), 1)
        	.shuffleGrouping("dispatcher");
        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(), 1)
        	.shuffleGrouping("checkopid");
        
        builder.setBolt("httpdispatcher", new HttpSubsDispatcherBolt(), 1)
        	.fieldsGrouping("subretriever", "httpSub", new Fields("subid"));
        builder.setBolt("pubsubdispatcher", new PubSubDispatcherBolt(), 1)
    		.fieldsGrouping("subretriever", "pubsubSub", new Fields("subid"));
        
        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(), 1)
    		.shuffleGrouping("subretriever", "internalSub");
        builder.setBolt("streamprocessor", new StreamProcessorBolt(qc), 1)
			.fieldsGrouping("streamdispatcher", new Fields("soid", "streamid"));
        
        
        Config conf = new Config();
        conf.setDebug(true);

    	conf.setMaxTaskParallelism(3);
    	LocalCluster cluster = new LocalCluster();
    	cluster.submitTopology("service-dispatcher", conf, builder.createTopology());
    	
    	feeder.feed(new Values("sometestopid", "temperature", 	"{" +
			 														"\"channels\":{" +
				 														"\"temperature\":{" +
				 															"\"current-value\": 36.4," +
				 															"\"unit\": \"celsius\"" +
				 														"}" +
			 														"}," +
			 														"\"lastUpdate\": 1392644627" +
			 													"}"));
    	
	}
}
