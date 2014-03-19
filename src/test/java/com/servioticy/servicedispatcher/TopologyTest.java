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
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.servioticy.datamodel.SOGroup;
import com.servioticy.datamodel.SUChannel;
import com.servioticy.datamodel.UpdateDescriptor;
import com.servioticy.dispatcher.DispatcherContext;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class TopologyTest {
	
	@Test
	public void testExampleTopology() {

		try{
			String opid = "sometestopid";
			String originSoid = "1234567890";
			String origStreamid = "location";
			String destSoid = "2345678901";

            DispatcherContext.loadConf(null);

            ObjectMapper mapper = new ObjectMapper();

            // Origin SO
            String originSo =		"{" +
										"\"streams\":{" +
											"\"location\": {" +
												"\"channels\": {" +
													"\"latitude\":{" +
														"\"type\": \"number\"" +
													"}," +
													"\"longitude\":{" +
														"\"type\": \"number\"" +
													"}" +
												"}" +
											"}" +
										"}" +
									"}";
			
			// Subscriptions document
			String subscriptions =	"{" +
										"\"subscriptions\": [" +
											"{" +
												"\"id\": \"subid1\", " +
												"\"callback\": \"internal\"," +
											    "\"destination\": \"" + destSoid + "\"," +
											    "\"customFields\": {" + 
											        "\"groupId\": \"group1\"" + 
											    "}" + 
										    "}," +
										    "{" +
											    "\"callback\":\"http\"," +
											    "\"destination\":\"http://172.20.200.229:5000/update/@latitude@/@longitude@\"," +
											    "\"customFields\": {" +
											      "\"aliases\": [" +
											        "{" +
											          "\"##\": \"{$.channels.\"," +
											          "\"!!\": \".current-value}\"" +
											        "}," +
											        "{\"@latitude@\": \"##latitude!!\"}," +
											        "{\"@longitude@\": \"##longitude!!\"}" +
											      "]," +
											      "\"method\":\"GET\"" +
											    "}" +
											"}" +
										 "]" +
									"}";
			// Subscriber SO
			String so =				"{" +
										"\"aliases\":[" +
											"{\"@nearDistance@\": \"0.0001\"}," +
											"{\"@latitude@\": \"channels.latitude.current-value\"}," +
											"{\"@longitude@\": \"channels.longitude.current-value\"}," +
											"{\"@latDistance@\": \"{$group1.@latitude@} - {$group2.@latitude@}\"}," +
											"{\"@longDistance@\": \"{$group1.@longitude@} - {$group2.@longitude@}\"}," +
											"{\"@distance@\": \"Math.sqrt(Math.pow(@latDistance@, 2) + Math.pow(@longDistance@, 2))\"}" +
										"]," +
										"\"groups\":{" +
											"\"group1\":{" +
												"\"soIds\":[" +
													"\"origin2\"," +
													"\"origin3\"" +
												"]," +
												"\"stream\": \"location\"" +
											"}," +
											"\"group2\":{" +
												"\"soIds\":[" +
													"\"origin4\"," +
													"\"origin5\"" +
												"]," +
												"\"stream\": \"location\"" +
											"}" +
										"}," +
										"\"streams\":{" +
											"\"proximity\": {" +
												"\"channels\": {" +
													"\"p\":{" +
														"\"current-value\": \"@distance@\"," +
														"\"type\": \"number\"" +
													"}" +
												"}," +
                    "\"post-filter\": \"{$proximity.} != null && {$proximity.channels.p.current-value} != {$result.channels.p.current-value}\"" +
                    "}," +
                    "\"near\":{" +
												"\"channels\": {" +
													"\"n\":{" +
														"\"current-value\": \"@distance@ <= @nearDistance@\"," +
														"\"type\": \"boolean\"" +
													"}" +
												"}," +
                    "\"post-filter\": \"{$near.} != null && {$near.channels.n.current-value} != {$result.channels.n.current-value}\"" +
                    "}" +
                    "}" +
									"}";
			
			// group1 SU
			String group1SU =	"{" +
									"\"channels\": {" +
										"\"latitude\":{" +
											"\"current-value\": 41.3879758" +
										"}," +
										"\"longitude\":{" +
											"\"current-value\": 2.1150167" +
										"}" +
									"}," +
									"\"lastUpdate\": 1392981962" +
								"}";
			
			// group2 SU
			String group2SU =	"{" +
									"\"channels\": {" +
										"\"latitude\":{" +
											"\"current-value\": 41.387975" +
										"}," +
										"\"longitude\":{" +
											"\"current-value\": 2.11501" +
										"}" +
									"}," +
									"\"lastUpdate\": 1392981836" +
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
					DispatcherContext.restBaseURL
					+ "private/" + opid, null,
					RestClient.GET,
					null)).thenReturn(new RestResponse("", 200));
			// get subscriptions
			when(restClient.restRequest(
					DispatcherContext.restBaseURL
					+ "private/" + originSoid + "/streams/"
					+ origStreamid
					+ "/subscriptions/", null, RestClient.GET,
					null)).thenReturn(new RestResponse(subscriptions, 200));
			// get origin so

			when(restClient.restRequest(
					DispatcherContext.restBaseURL
					+ "private/" + originSoid, null, RestClient.GET,
					null)).thenReturn(new RestResponse(originSo, 200));
			// get subscriber so

			when(restClient.restRequest(
					DispatcherContext.restBaseURL
					+ "private/" + destSoid, null, RestClient.GET,
					null)).thenReturn(new RestResponse(so, 200));
			// get 'group2' location group last update
			when(restClient.restRequest(
					DispatcherContext.restBaseURL
					+ "private/groups/lastUpdate", mapper.writeValueAsString(group), RestClient.POST,
					null)).thenReturn(new RestResponse(group2SU, 200));
			// get 'proximity' stream last update
					when(restClient.restRequest(
							DispatcherContext.restBaseURL
							+ "private/" + destSoid + "/streams/proximity/lastUpdate", null, RestClient.GET,
							null)).thenReturn(new RestResponse(proxSU, 200));
			// get 'near' stream last update
			when(restClient.restRequest(
					DispatcherContext.restBaseURL
					+ "private/" + destSoid + "/streams/near/lastUpdate", null, RestClient.GET,
					null)).thenReturn(new RestResponse(nearSU, 200));
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
			
	        builder.setBolt("checkopid", new CheckOpidBolt(restClient), 1)
	        	.shuffleGrouping("dispatcher");
	        builder.setBolt("subretriever", new SubscriptionRetrieveBolt(restClient), 1)
        		.shuffleGrouping( "checkopid", "subscription");
	        
	        builder.setBolt("httpdispatcher", new HttpSubsDispatcherBolt(), 1)
	        	.fieldsGrouping("subretriever", "httpSub", new Fields("subid"));
	        builder.setBolt("pubsubdispatcher", new PubSubDispatcherBolt(), 1)
	    		.fieldsGrouping("subretriever", "pubsubSub", new Fields("subid"));
	        
	        builder.setBolt("streamdispatcher", new StreamDispatcherBolt(restClient), 1)
	    		.shuffleGrouping("subretriever", "internalSub")
	    		.shuffleGrouping("checkopid", "stream");
	        builder.setBolt("streamprocessor", new StreamProcessorBolt(qc, restClient), 1)
				.fieldsGrouping("streamdispatcher", new Fields("soid", "streamid"));
	        
	        
	        Config conf = new Config();
	        conf.setDebug(true);
	
	    	LocalCluster cluster = new LocalCluster();
	    	cluster.submitTopology("dispatcher", conf, builder.createTopology());
	    	
	    	feeder.feed(new Values("sometestopid", "1234567890", "location", group1SU));
	    	
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
	    	Assert.assertTrue("Origin SO id", ud.getSoid().equals(destSoid));
	    	Assert.assertTrue("Origin stream id", ud.getStreamid().equals("proximity"));
	    	Assert.assertTrue("New SU timestamp", ud.getSu().getLastUpdate() == 1392981962);
	    	SUChannel such = ud.getSu().getChannels().get("p");
	    	double proximity = (Double)such.getCurrentValue();
	    	Assert.assertTrue("New SU current-value", proximity >= 0.000006747);
	    	Assert.assertTrue("New SU current-value", proximity <= 0.000006748);
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
