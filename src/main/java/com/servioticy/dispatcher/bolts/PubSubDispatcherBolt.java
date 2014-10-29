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

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.reputation.Reputation;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.servioticy.datamodel.subscription.PubSubSubscription;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SUCache;
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
	private DispatcherContext dc;
	private static Logger LOG = org.apache.log4j.Logger.getLogger(PubSubDispatcherBolt.class);
	
	
	public PubSubDispatcherBolt(DispatcherContext dc){
        this.dc = dc;
	}
	
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.context = context;
		this.publisher = null;
		this.suCache = new SUCache(25);
		
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
		ObjectMapper mapper = new ObjectMapper();
		PubSubSubscription externalSub;
        String suDoc = input.getStringByField("su");
        String subsDoc = input.getStringByField("subsdoc");
        String sourceSOId = input.getStringByField("soid");
        String streamId = input.getStringByField("streamid");

        SensorUpdate su;
		try{
			su = mapper.readValue(suDoc,
					SensorUpdate.class);
			externalSub = mapper.readValue(subsDoc,
                    PubSubSubscription.class);
			if(suCache.check(externalSub.getId(), su.getLastUpdate())){
				// This SU or a posterior one has already been sent, do not send this one.
				collector.ack(input);
				return;
			}
		}catch (Exception e) {
			LOG.error("FAIL", e);
			collector.fail(input);
			return;
		}
        String topic = externalSub.getDestination()+"/"+sourceSOId+"/streams/"+streamId+"/updates";
		try {
			publisher.publishMessage(topic, suDoc);
			LOG.info("PubSub message pubished on topic "+topic);
		} catch (Exception e) {
			LOG.error("FAIL", e);
			collector.fail(input);
			return;
		}
        this.collector.emit(Reputation.STREAM_SO_PUBSUB, input,
                new Values(sourceSOId,
                        streamId,
                        topic,
                        su.getLastUpdate(),
                        System.currentTimeMillis()
                )
        );
		suCache.put(externalSub.getId(), su.getLastUpdate());
		collector.ack(input);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Reputation.STREAM_SO_PUBSUB, new Fields("in-soid", "in-streamid", "out-topic", "user_timestamp", "date"));

    }

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
