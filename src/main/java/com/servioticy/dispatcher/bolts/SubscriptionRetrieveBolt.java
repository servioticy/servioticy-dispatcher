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

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.servioticy.datamodel.Subscription;
import com.servioticy.datamodel.Subscriptions;
import com.servioticy.dispatcher.DispatcherContext;

import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestResponse;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class SubscriptionRetrieveBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private RestClient restClient;
	
	public SubscriptionRetrieveBolt(){
	}
	
	// For testing purposes
	public SubscriptionRetrieveBolt(RestClient restClient){
		this.restClient = restClient;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		if(restClient == null){
			restClient = new RestClient();
		}
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		Subscriptions subscriptions;
		RestResponse subscriptionsRR;
		Map<String, String> headers = new HashMap<String, String>();

		try {
			subscriptionsRR = restClient.restRequest(
					DispatcherContext.restBaseURL
							+ input.getStringByField("soid") + "/streams/"
							+ input.getStringByField("streamid")
							+ "/subscriptions/", null, RestClient.GET,
							null);
		} catch (RestClientErrorCodeException e) {
			// In case there are no subscriptions.
			if(e.getRestResponse().getHttpCode() == 204){
				this.collector.ack(input);
				return;
			} 
			else{
				// TODO Log the error
				// Retry until timeout
				this.collector.fail(input);
				return;
			}
		} catch (Exception e) {
			// TODO Log the error
			// Retry until timeout
			this.collector.fail(input);
			return;
		}
		
		try {
			subscriptions = mapper.readValue(subscriptionsRR.getResponse(),
					Subscriptions.class);
		} catch (Exception e) {
			// TODO Log the error
			collector.ack(input);
			return;
		}
		
		// No subscriptions
		if(subscriptions.getSubscriptions() == null || subscriptions.getSubscriptions().isEmpty()){
			collector.ack(input);
			return;
		}
		
		for (Subscription subscription : subscriptions
				.getSubscriptions()) {
			try {
				if(subscription.getCallback().equals(Subscription.SUBS_INTERNAL)){
					this.collector.emit(	"internalSub", input, 
							new Values(	mapper.writeValueAsString(subscription),
										input.getStringByField("su")));
				}
				else if(subscription.getCallback().equals(Subscription.SUBS_HTTP)){
					this.collector.emit(	"httpSub", input, 
							new Values(	subscription.getId(),
										mapper.writeValueAsString(subscription),
										input.getStringByField("su")));
				}
				else if(subscription.getCallback().equals(Subscription.SUBS_PUBSUB)){
					this.collector.emit(	"pubsubSub", input, 
							new Values(	subscription.getId(),
										mapper.writeValueAsString(subscription),
										input.getStringByField("su")));
				}
			} catch (Exception e) {
				// TODO Log the error
			}
		}
		collector.ack(input);
		return;
	}

	public void cleanup() {		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("internalSub", new Fields("subsdoc", "su"));
		declarer.declareStream("httpSub", new Fields("subid", "subsdoc", "su"));
		declarer.declareStream("pubsubSub", new Fields("subid", "subsdoc", "su"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
