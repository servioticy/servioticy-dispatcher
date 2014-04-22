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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestResponse;

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
    private DispatcherContext dc;

	public CheckOpidBolt(DispatcherContext dc) {
        this.dc = dc;
	}

	// For testing purposes
	public CheckOpidBolt(DispatcherContext dc, RestClient restClient) {
        this.dc = dc;
		this.restClient = restClient;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		if (restClient == null) {
			restClient = new RestClient();
		}

	}

	public void execute(Tuple input) {
		

		RestResponse rr;
		String opid = input.getStringByField("opid");

		try {
			rr = restClient.restRequest(
					dc.restBaseURL
							+ "private/opid/" + opid, null,
					RestClient.GET, null);

		} catch (Exception e) {
			// TODO Log the error
			// Retry until timeout
			this.collector.fail(input);
			return;
		}
	
		this.collector.emit(
				"stream",
				input,
				new Values(null, input
						.getStringByField("soid"), input
						.getStringByField("streamid"), input
						.getStringByField("su")));

		this.collector.emit(
				"subscription",
				input,
				new Values(input
						.getStringByField("soid"), input
						.getStringByField("streamid"), input
						.getStringByField("su")));



		this.collector.ack(input);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("subscription", new Fields("soid", "streamid", "su"));
		declarer.declareStream("stream", new Fields("subsdoc","soid", "streamid", "su"));
		declarer.declareStream("actuation", new Fields("soid", "id", "action"));

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
