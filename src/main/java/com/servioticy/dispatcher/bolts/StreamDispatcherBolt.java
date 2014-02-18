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

import com.servioticy.datamodel.SOSubscription;
import com.servioticy.dispatcher.SDispatcherContext;
import com.servioticy.dispatcher.jsonprocessors.SOProcessor;

import com.servioticy.restclient.RestClient;
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
public class StreamDispatcherBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private RestClient restClient;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.restClient = new RestClient();
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		SOSubscription soSub;
		String sostr;
		RestResponse rr;
		
		try {
			soSub = mapper.readValue(input.getStringByField("subsdoc"),
					SOSubscription.class);
		} catch (Exception e) {
			// TODO Log the error
			collector.ack(input);
			return;
		}
		try{
			rr = restClient.restRequest(
					SDispatcherContext.restBaseURL
						+ soSub.getDestination(), null, RestClient.GET,
						null);
			sostr = rr.getResponse();
		} catch (Exception e) {
			// TODO Log the error
			collector.fail(input);
			return;
		}
		// TODO Could be useful to delete the unused groups from the SO. Open discussion.
		try{
			SOProcessor sop = new SOProcessor(sostr, soSub.getDestination());
			sostr = sop.replaceAliases();
			sop.compileJSONPaths();
			for( String streamId: sop.getStreamsByDocId( soSub.getGroupId() ) ){
				this.collector.emit(	input, 
										new Values(	soSub.getDestination(),
													streamId,
													sostr,
													soSub.getGroupId(),
													input.getStringByField("su")));
			}
		}catch(Exception e){
			collector.ack(input);
			//TODO Log the error
			return;
		}
		collector.ack(input);
		return;
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("soid", "streamid", "so", "groupid", "su"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
