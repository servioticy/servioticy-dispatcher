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

import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.restclient.RestClient;

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
public class CheckOpidBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private RestClient restClient;

	public CheckOpidBolt(){
	}
	
	// For testing purposes
	public CheckOpidBolt(RestClient restClient){
		this.restClient = restClient;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		if(restClient == null){
			restClient = new RestClient();
		}

	}

	public void execute(Tuple input) {
		try {
			restClient.restRequest(
					DispatcherContext.restBaseURL
							+ input.getStringByField("opid"), null,
					RestClient.GET,
					null);

		} catch (Exception e) {
			// TODO Log the error
			// Retry until timeout
			this.collector.fail(input);
			return;
		}

		this.collector.emit(
				input,
				new Values(input.getStringByField("opid"), 
						input.getStringByField("auth"), input
						.getStringByField("soid"), input
						.getStringByField("streamid"), input
						.getStringByField("su")));
		this.collector.ack(input);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("opid", "auth", "soid", "streamid", "su"));

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
