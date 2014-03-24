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

import com.ibm.sr.exceptions.SRException;
import com.servioticy.datamodel.ExternalSubscription;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.SRPublisher;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.dispatcher.jsonprocessors.PubSubProcessor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

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
	private SRPublisher srpub;
	private SUCache suCache;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.srpub = null;
		this.suCache = new SUCache(25);
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		ExternalSubscription externalSub;
		SensorUpdate su;
		try{
			su = mapper.readValue(input.getStringByField("su"),
					SensorUpdate.class);
			externalSub = mapper.readValue(input.getStringByField("subsdoc"),
					ExternalSubscription.class);
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
		if(srpub == null || pubsubp.getUrl(suStr) != srpub.getTopicName()){
			if(pubsubp.getUrl(suStr) != srpub.getTopicName()){
				try {
					// wait for everything to propagate
					Thread.sleep(1000);
					srpub.close();
				} catch (Exception e) {
					// TODO Log this.
				}
			}
			
			try {
				srpub = new SRPublisher(pubsubp.getUrl(suStr), String.valueOf(context.getThisTaskId()));
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
			srpub.publishMessage(pubsubp.getBody(suStr));
		} catch (SRException e) {
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
