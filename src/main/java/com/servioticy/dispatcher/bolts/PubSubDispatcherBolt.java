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

import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.servioticy.datamodel.PubSubSubscription;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.dispatcher.jsonprocessors.PubSubProcessor;
import com.servioticy.dispatcher.pubsub.PubSubPublisherFactory;
import com.servioticy.dispatcher.pubsub.PublisherInterface;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class PubSubDispatcherBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private PublisherInterface publisher;
	private SUCache suCache;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.publisher = null;
		this.suCache = new SUCache(25);
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		PubSubSubscription externalSub;
		SensorUpdate su;
		String sourceSOId;
		try{
			su = mapper.readValue(input.getStringByField("su"),
					SensorUpdate.class);
			externalSub = mapper.readValue(input.getStringByField("subsdoc"),
					PubSubSubscription.class);
			sourceSOId = input.getStringByField("soid");
			if(suCache.check(externalSub.getId(), su.getLastUpdate())){
				// This SU or a posterior one has already been sent, do not send this one.
				collector.ack(input);
				return;
			}
		}catch (Exception e) {
			// TODO Log the error
			collector.ack(input);
			return;
		}
		PubSubProcessor pubsubp = new PubSubProcessor(externalSub);
		pubsubp.replaceAliases();
		pubsubp.compileJSONPaths();
		String suStr = input.getStringByField("su");
		//if(publisher == null || pubsubp.getUrl(suStr) != publisher.getTopicName()){
		if(publisher == null){
				/*if(pubsubp.getUrl(suStr) != publisher.getTopicName()){
					try {
						// wait for everything to propagate
						Thread.sleep(1000);
						publisher.close();
					} catch (Exception e) {
						// TODO Log this.
					}
				}
			}*/
			try {
				publisher = PubSubPublisherFactory.getPublisher(String.valueOf(context.getThisTaskId()));
			} catch (Exception e) {
				// TODO Log the error
				collector.fail(input);
				return;
			}
			// wait for everything to propagate
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				// TODO Log this.
			}
		}
		try {
			publisher.publishMessage(pubsubp.getUrl(suStr)+"/"+sourceSOId+"/updates", pubsubp.getBody(suStr));
		} catch (Exception e) {
			// TODO Log the error
			collector.fail(input);
			return;
		}
		suCache.put(externalSub.getId(), su.getLastUpdate());
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
