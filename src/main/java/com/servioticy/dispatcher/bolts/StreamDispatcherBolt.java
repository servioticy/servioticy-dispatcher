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
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;

import com.servioticy.datamodel.SO;
import com.servioticy.datamodel.SOGroup;
import com.servioticy.datamodel.SOSubscription;
import com.servioticy.dispatcher.DispatcherContext;
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
	
	public StreamDispatcherBolt(){
	}
	
	// For testing purposes
	public StreamDispatcherBolt(RestClient restClient){
		this.restClient = restClient;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		if(restClient == null){
			restClient = new RestClient();
		}
	}

	public void execute(Tuple input) {
		ObjectMapper mapper = new ObjectMapper();
		SOSubscription soSub = null;
		SO so;
		RestResponse rr;
		
		String subsDoc = input.getStringByField("subsdoc");
		String suDoc = input.getStringByField("su");
		String soId = input.getStringByField("soid");
		String streamId = input.getStringByField("streamid");
		String soDoc;
		
		String destination;
		if(subsDoc != null){
			// This SU comes from a subscription.
			try {
				soSub = mapper.readValue(subsDoc,
						SOSubscription.class);
			} catch (Exception e) {
				// TODO Log the error
				e.printStackTrace();
				collector.ack(input);
				return;
			}
			destination = soSub.getDestination();
		}
		else{
			destination = soId;
		}

		try{
			rr = restClient.restRequest(
					DispatcherContext.restBaseURL
						+ "private/" + destination, null, RestClient.GET,
						null);
			soDoc = rr.getResponse();
		} catch (Exception e) {
			// TODO Log the error
			e.printStackTrace();
			collector.fail(input);
			return;
		}
		try{
			so = mapper.readValue(soDoc,
					SO.class);
		} catch (Exception e) {
			// TODO Log the error
			e.printStackTrace();
			collector.ack(input);
			return;
		}
		
		String docId;
		if(soSub == null){ 
			if(so.getGroups() != null){
				for(Entry<String, SOGroup> group: so.getGroups().entrySet()){
					// If there is a group called like the stream, then the docname refers to the group.
					if(group.getKey() == streamId){
						collector.ack(input);
						return;
					}
				}
			}
			
			docId = streamId;
		}
		else{
			docId = soSub.getGroupId();
		}
		
		// TODO Could be useful to delete the unused groups from the SO. Open discussion.
		try{
			SOProcessor sop = new SOProcessor(soDoc, destination);
			soDoc = sop.replaceAliases();
			sop.compileJSONPaths();
			for( String streamIdByDoc: sop.getStreamsByDocId( docId ) ){
				this.collector.emit(	input, 
										new Values(	soSub.getDestination(),
													streamIdByDoc,
													soDoc,
													soSub.getGroupId(),
													suDoc));
			}
		}catch(Exception e){
			collector.ack(input);
			//TODO Log the error
			e.printStackTrace();
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
