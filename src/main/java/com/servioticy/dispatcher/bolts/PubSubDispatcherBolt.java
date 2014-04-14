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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import clojure.lang.PersistentArrayMap;

import com.servioticy.datamodel.PubSubSubscription;
import com.servioticy.datamodel.SensorUpdate;
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
	private static Logger LOG = org.apache.log4j.Logger.getLogger(PubSubDispatcherBolt.class);
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.publisher = null;
		this.suCache = new SUCache(25);
		
		PersistentArrayMap mqttOptions = (PersistentArrayMap) stormConf.get("mqttconfig");
		
		Iterator iter = mqttOptions.values().iterator();
		PersistentArrayMap props;
		List<String> uris = new ArrayList<String>();
		while( iter.hasNext() ) {
			props = (PersistentArrayMap)iter.next();
        	if(props.containsKey(DispatcherContext.MQTT_URI))
        		LOG.debug("MQTT server: " + props.get(DispatcherContext.MQTT_URI));
        	        	
        	if(props.containsKey(DispatcherContext.MQTT_USER) &&
             	   props.containsKey(DispatcherContext.MQTT_PASS))
             		LOG.debug("user/pass: " + props.get(DispatcherContext.MQTT_USER) +
             		"/"+props.get(DispatcherContext.MQTT_PASS));
        
        	
        	String uri = (String)props.get(DispatcherContext.MQTT_URI);

        	
        	
        	try {
				publisher = PubSubPublisherFactory.getPublisher(uri, String.valueOf(context.getThisTaskId()));
			} catch (Exception e) {
				LOG.error("Prepare: ", e);
			}
        	
			uris.add(uri);
			String[] urisArray = new String[uris.size()];
        	publisher.connect(uris.toArray(urisArray), 
        					  (String)props.get(DispatcherContext.MQTT_USER), 
        					  (String)props.get(DispatcherContext.MQTT_PASS));	
		}		
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		PubSubSubscription externalSub;
		SensorUpdate su;
		String sourceSOId;
		String streamId;
		try{
			su = mapper.readValue(input.getStringByField("su"),
					SensorUpdate.class);
			externalSub = mapper.readValue(input.getStringByField("subsdoc"),
					PubSubSubscription.class);
			sourceSOId = input.getStringByField("soid");
			streamId = input.getStringByField("streamid");
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
		//PubSubProcessor pubsubp = new PubSubProcessor(externalSub);
		//pubsubp.replaceAliases();
		//pubsubp.compileJSONPaths();
		String suStr = input.getStringByField("su");
		try {
			publisher.publishMessage(externalSub.getDestination()+"/"+sourceSOId+"/streams/"+streamId+"/updates", suStr);
			LOG.info("PubSub message pubished on topic "+externalSub.getDestination()+"/"+sourceSOId+"/streams/"+streamId+"/updates");
		} catch (Exception e) {
			LOG.error("FAIL", e);
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
