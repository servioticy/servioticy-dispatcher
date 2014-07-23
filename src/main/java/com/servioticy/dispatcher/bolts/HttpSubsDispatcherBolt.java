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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.HttpSubscription;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.dispatcher.jsonprocessors.HttpSubProcessor;

import com.servioticy.restclient.RestClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class HttpSubsDispatcherBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private SUCache suCache;
	private RestClient restClient;
	
	public HttpSubsDispatcherBolt(){
	}
	
	// For testing purposes
	public HttpSubsDispatcherBolt(RestClient restClient){
		this.restClient = restClient;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.suCache = new SUCache(25);
		if(restClient == null){
			restClient = new RestClient();
		}
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		HttpSubscription httpSub;
		SensorUpdate su;
		
		try{
			su = mapper.readValue(input.getStringByField("su"),
					SensorUpdate.class);
			httpSub = mapper.readValue(input.getStringByField("subsdoc"),
					HttpSubscription.class);
			if(suCache.check(httpSub.getId(), su.getLastUpdate())){
				// This SU or a posterior one has already been sent, do not send this one.
				collector.ack(input);
				return;
			}
			
		}catch (Exception e) {
			// TODO Log the error
			collector.ack(input);
			return;
		}
		HttpSubProcessor hsp = new HttpSubProcessor(httpSub);
		hsp.replaceAliases();
		hsp.compileJSONPaths();
		String suStr = input.getStringByField("su");
		String url = hsp.getUrl(suStr);
		String body = hsp.getBody(suStr);
		int method = hsp.getMethod();
		HashMap<String, String> headers = hsp.getHeaders(suStr);
		try {
			restClient.restRequest(url
					, body, method,
					headers);
		} catch (Exception e) {
			// The problem is probably out of our boundaries.
			// TODO Log the error
		} 
		suCache.put(httpSub.getId(), su.getLastUpdate());
		collector.ack(input);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
