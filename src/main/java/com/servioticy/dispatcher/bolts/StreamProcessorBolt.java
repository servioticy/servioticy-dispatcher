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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import com.servioticy.datamodel.SO;
import com.servioticy.datamodel.SOGroup;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.dispatcher.jsonprocessors.SOProcessor;
import com.servioticy.queueclient.QueueClient;

import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestClientException;
import com.servioticy.restclient.RestResponse;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class StreamProcessorBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private SUCache suCache;
	private QueueClient qc;
	private RestClient restClient;
	
	public StreamProcessorBolt(){
		
	}
	
	// For testing purposes
	public StreamProcessorBolt(QueueClient qc, RestClient restClient){
		this.qc = qc;
		this.restClient = restClient;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.context = context;
		this.suCache = new SUCache(25);
		if(restClient == null){
			restClient = new RestClient();
		}
	}
	
	private Map<String, String> getGroupDocs(Set<String> docIds, String soId, SO so) throws IOException, RestClientException, RestClientErrorCodeException{
		RestResponse rr;
		Map<String, String> groupDocs = new HashMap<String, String>();
		ObjectMapper mapper = new ObjectMapper();

        if(so.getGroups() == null){
            return groupDocs;
        }

		for(String docId: docIds){
			if(!so.getGroups().containsKey(docId)){
				continue;
			}
			SOGroup group = so.getGroups().get(docId);
			// TODO Resolve dynsets
			String lastSU;
			String glurstr = mapper.writeValueAsString(group);
			
			rr = restClient.restRequest(
					DispatcherContext.restBaseURL
						+ "private/groups/lastUpdate", 
						glurstr, RestClient.POST,
						null);
			// In case there is no update.
			if(rr.getHttpCode() == 204){
				groupDocs.put(docId, "null");
				continue;
			}
			lastSU = rr.getResponse();

			groupDocs.put(docId, lastSU);
			
		}
		
		return groupDocs;
	}
	
	private Map<String, String> getStreamDocs(Set<String> docIds, String soId, SO so) throws IOException, RestClientException, RestClientErrorCodeException{
		RestResponse rr;
		Map<String, String> streamDocs = new HashMap<String, String>();
		ObjectMapper mapper = new ObjectMapper();	
		for(String docId: docIds){
			if(!so.getStreams().containsKey(docId)){
				continue;
			}
			String lastSU;
			rr = restClient.restRequest(
					DispatcherContext.restBaseURL
						+ "private/" + soId + "/streams/" + docId + "/lastUpdate", 
						null, RestClient.GET,
						null);

			// In case there is no update.
			if(rr.getHttpCode() == 204){
				streamDocs.put(docId, "null");
				continue;
			}
			lastSU = rr.getResponse();
			// TODO If there is not a lastSU, don't put it.
			streamDocs.put(docId, lastSU);
		}
		
		return streamDocs;
	}
	
	public void execute(Tuple input) {
		RestResponse rr;
		SensorUpdate su;
		SensorUpdate previousSU;
		String previousSUDoc;
		SO so;
		ObjectMapper mapper = new ObjectMapper();
		String soId = input.getStringByField("soid");
		String streamId = input.getStringByField("streamid");
		String suDoc = input.getStringByField("su");
		String soDoc = input.getStringByField("so");
		String groupId = input.getStringByField("groupid");
		SOProcessor sop;
		long timestamp;
		Map<String, String> docs;
		
		try{
			su = mapper.readValue(suDoc, SensorUpdate.class);
			so = mapper.readValue(soDoc, SO.class);	
			sop = new SOProcessor(soDoc, soId);
		} catch(Exception e){
			// TODO Log the error
			e.printStackTrace();
			collector.ack(input);
			return;
		}
		if(suCache.check(soId + ";" + streamId, su.getLastUpdate())){
			// This SU or a posterior one has already been sent, do not send this one.
			collector.ack(input);
			return;
		}
		// It is not needed to replace the alias, it has been already done in the previous bolt.
		sop.compileJSONPaths();
		
		Set<String> docIds = sop.getDocIdsByStream(streamId);
		// Remove the group for which we already have the SU
		docIds.remove(groupId);
		// The self last update from current stream
		docIds.add(streamId);

		docs = new HashMap<String, String>();
		docs.put("", suDoc);
		try{
			docs.putAll(this.getStreamDocs(docIds, soId, so));
			docs.putAll(this.getGroupDocs(docIds, soId, so));
			docs.put(groupId, suDoc);
		} catch(Exception e){
			// TODO Log the error
			e.printStackTrace();
			collector.fail(input);
			return;
		}
		// Obtain the highest timestamp from the input docs
		timestamp = su.getLastUpdate();
		for(Map.Entry<String, String> doc: docs.entrySet()){
			SensorUpdate inputSU;
			try{
				String docContent = doc.getValue();
				if(docContent.equals("null")){
					continue;
				}
				inputSU = mapper.readValue(docContent, SensorUpdate.class);
			} catch(Exception e){
				// TODO Log the error
				e.printStackTrace();
				collector.ack(input);
				return;
			}
			timestamp = inputSU.getLastUpdate() > timestamp ? inputSU.getLastUpdate() : timestamp;
		}
		
		previousSUDoc = docs.get(streamId);
		if(!previousSUDoc.equals("null")){
			try{
				previousSU = mapper.readValue(previousSUDoc, SensorUpdate.class);
			} catch(Exception e){
				// TODO Log the error
				e.printStackTrace();
				collector.ack(input);
				return;
			}
			// There is already a newer update stored
			if(timestamp <= previousSU.getLastUpdate()){
				collector.ack(input);
				return;
			}
		}
		
		String resultSUDoc;
		try{
			if(!sop.checkPreFilter(streamId, docs)){
				collector.ack(input);
				return;
			}
			
			SensorUpdate resultSU = sop.getResultSU(streamId, docs, timestamp);
			if(resultSU == null){
				collector.ack(input);
				return;
			}
			mapper.setSerializationInclusion(Inclusion.NON_NULL);
			resultSUDoc = mapper.writeValueAsString(resultSU);

            if(!docs.containsKey("result")){
                docs.put("result", resultSUDoc);
            }

            // Deprecated. Only for retrocompatibility
            if(!docs.containsKey("@result@")){
                docs.put("@result@", resultSUDoc);
            }
			
			if(!sop.checkPostFilter(streamId, docs)){
				collector.ack(input);
				return;
			}
		} catch(Exception e){
			// TODO Log the error
			e.printStackTrace();
			collector.ack(input);
			return;
		}
		
		// generate opid
		String opid = Integer.toHexString(resultSUDoc.hashCode());
		
		// The output dispatcher json
		String dispatcherJson =	"{"+
									"\"opid\":\"" + opid + "\"," +
									"\"soid\":\"" + soId + "\"," +
									"\"streamid\":\"" + streamId + "\"," +
									"\"su\":" + resultSUDoc +
								"}";
		
		// Put to the queue
		if(this.qc == null){
			try{
				qc = QueueClient.factory();
			} catch(Exception e){
				// TODO Log the error
				collector.ack(input);
				return;
			}
		}
		try{
			qc.connect();
			if(!qc.put(dispatcherJson)){
				// TODO Log the error
				collector.fail(input);
				return;
			}
			qc.disconnect();
		} catch (Exception e) {
			// TODO Log the error
			collector.fail(input);
			return;
		}
		
		try{
			// Send to the API
			restClient.restRequest(
					DispatcherContext.restBaseURL
							+ "private/" + soId + "/streams/"
							+ streamId + "/" + opid, resultSUDoc,
					RestClient.PUT,
					null);
		} catch(Exception e){
			// TODO Log the error
			collector.fail(input);
			return;
		}
		
		suCache.put(soId+";"+streamId, su.getLastUpdate());
		collector.ack(input);
		return;
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
