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

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.esotericsoftware.minlog.Log;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.pubsub.PubSubPublisherFactory;
import com.servioticy.dispatcher.pubsub.PublisherInterface;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class ActuationDispatcherBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private PublisherInterface publisher;
	private DispatcherContext dc;
	private static Logger LOG = org.apache.log4j.Logger.getLogger(ActuationDispatcherBolt.class);
	private ObjectMapper mapper;
	
	public ActuationDispatcherBolt(DispatcherContext dc){
        this.dc = dc;
	}
	
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.mapper = new ObjectMapper();
		this.collector = collector;
		this.context = context;
		this.publisher = null;
		
		LOG.debug("MQTT server: " + dc.mqttUri);
		LOG.debug("MQTT user/pass: " + dc.mqttUser+ " / "+dc.mqttPassword);
		
		

		try {
			publisher = PubSubPublisherFactory.getPublisher(dc.mqttUri, String.valueOf(context.getThisTaskId()));
		} catch (Exception e) {
			LOG.error("Prepare: ", e);
		}

		publisher.connect(dc.mqttUri, 
				dc.mqttUser, 
				dc.mqttPassword);	
		
	}

	public void execute(Tuple input) {
		
		try {
			String sourceSOId;
			String actionId;
			String actionName;
			
			JsonNode actuation = this.mapper.readTree(input.getStringByField("action"));
			sourceSOId = input.getStringByField("soid");
			actionId = input.getStringByField("id");
			actionName = input.getStringByField("name");
			Log.info("received actuation\n\t\t" +
					 "action: "+actuation.toString()+"\n\t\t"+
					 "soid: "+sourceSOId+"\n\t\t"+
					 "name: "+actionName+"\n\t\t"+
					 "id: "+actionId);


			if(!publisher.isConnected()){
				publisher.connect(dc.mqttUri,
						dc.mqttUser,
						dc.mqttPassword);
			}
			publisher.publishMessage(sourceSOId+"/actions", actuation.toString());
			LOG.info("Actuation request pubished on topic "+sourceSOId+"/actions");
			LOG.info("Actuation message contents: "+actuation.toString());
			
		} catch (Exception e) {
			LOG.error("FAIL", e);
			collector.fail(input);
			return;
		}
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
