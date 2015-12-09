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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.servioticy.datamodel.subscription.ExternalSubscription;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.dispatcher.publishers.Publisher;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class ExternalDispatcherBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private Publisher publisher;
	private SUCache suCache;
	private DispatcherContext dc;
	private static Logger LOG = org.apache.log4j.Logger.getLogger(ExternalDispatcherBolt.class);
	private ObjectMapper mapper;

	public ExternalDispatcherBolt(DispatcherContext dc){
        this.dc = dc;
	}
	
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.mapper = new ObjectMapper();
		this.collector = collector;
		this.context = context;
		this.publisher = null;
		this.suCache = new SUCache(25);
		
		LOG.debug("External publisher server: " + dc.externalPubAddress + ":" + dc.externalPubPort);
		LOG.debug("External publisher user/pass: " + dc.externalPubUser + " / "+dc.externalPubPassword);

		
		try {
			publisher = Publisher.factory(dc.externalPubClassName, dc.externalPubAddress, dc.externalPubPort, String.valueOf((context.getStormId() + String.valueOf(context.getThisTaskId())).hashCode()));
		} catch (Exception e) {
			LOG.error("Prepare: ", e);
			throw new RuntimeException();
		}
		try {
			publisher.connect(dc.externalPubUser, dc.externalPubPassword);
		} catch (Exception e) {
			LOG.error("Prepare: ", e);
		}

	}

	public void execute(Tuple input) {
		ExternalSubscription externalSub;
		SensorUpdate su;
		String sourceSOId;
		String streamId;
		try{
			su = mapper.readValue(input.getStringByField("su"),
					SensorUpdate.class);
			externalSub = mapper.readValue(input.getStringByField("subsdoc"),
					ExternalSubscription.class);
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

		String suStr = input.getStringByField("su");
		try {
			if (!publisher.isConnected()) {
				publisher.connect(dc.externalPubUser,
						dc.externalPubPassword);
			}
			publisher.publishMessage(externalSub.getDestination()+"/"+sourceSOId+"/streams/"+streamId+"/updates", suStr);
			LOG.info("Message pubished on topic "+externalSub.getDestination()+"/"+sourceSOId+"/streams/"+streamId+"/updates");
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
