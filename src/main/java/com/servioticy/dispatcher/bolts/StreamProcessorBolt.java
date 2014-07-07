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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.servioticy.datamodel.SO;
import com.servioticy.datamodel.SOGroup;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SOUtils;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.queueclient.KestrelThriftClient;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestClientException;
import com.servioticy.restclient.RestResponse;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import java.io.IOException;
import java.util.*;

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
    private DispatcherContext dc;
	
	public StreamProcessorBolt(){
		
	}
	
	// For testing purposes
	public StreamProcessorBolt(DispatcherContext dc, QueueClient qc, RestClient restClient){
        this.dc = dc;
		this.qc = qc;
		this.restClient = restClient;
	}
    public StreamProcessorBolt(DispatcherContext dc, RestClient restClient){
        this.restClient = restClient;
        this.dc = dc;
        String kestrelAddresses = "";
        for (String addr : dc.kestrelAddresses) {
            kestrelAddresses += addr + ":" + dc.kestrelPort + " ";
        }
        KestrelThriftClient ktc = new KestrelThriftClient();

        ktc.setBaseAddress(kestrelAddresses);
        ktc.setRelativeAddress(dc.kestrelQueue);
        ktc.setExpire(0);
        this.qc = ktc;
    }
    public StreamProcessorBolt(DispatcherContext dc, QueueClient qc){
        this.dc = dc;
        this.qc = qc;
    }
    public StreamProcessorBolt(DispatcherContext dc){
        this(dc, (RestClient) null);
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

    private Map<String, String> getGroupSUs(Set<String> docIds, SO so) throws IOException, RestClientException, RestClientErrorCodeException {
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

            try {
                rr = restClient.restRequest(
                        dc.restBaseURL
                                + "private/groups/lastUpdate",
                        glurstr, RestClient.POST,
                        null
                );
            } catch (RestClientErrorCodeException e) {
                // TODO Log the error
                e.printStackTrace();
                if (e.getRestResponse().getHttpCode() >= 500) {
                    throw e;
                }
                groupDocs.put(docId, "null");
                continue;
            }
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

    private Map<String, String> getStreamSUs(Set<String> docIds, SO so) throws IOException, RestClientException, RestClientErrorCodeException {
        RestResponse rr;
		Map<String, String> streamDocs = new HashMap<String, String>();
		ObjectMapper mapper = new ObjectMapper();	
		for(String docId: docIds){
			if(!so.getStreams().containsKey(docId)){
				continue;
			}
			String lastSU;
            try {
                rr = restClient.restRequest(
                        dc.restBaseURL
                                + "private/" + so.getId() + "/streams/" + docId + "/lastUpdate",
                        null, RestClient.GET,
                        null
                );
            } catch (RestClientErrorCodeException e) {
                // TODO Log the error
                e.printStackTrace();
                if (e.getRestResponse().getHttpCode() >= 500) {
                    throw e;
                }
                streamDocs.put(docId, "null");
                continue;
            }
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
        String originId = input.getStringByField("originid");
        SOUtils sou;
        long timestamp;
        Map<String, String> docs;

        try {
            su = mapper.readValue(suDoc, SensorUpdate.class);
            so = mapper.readValue(soDoc, SO.class);
            sou = new SOUtils(so);
        } catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
            collector.ack(input);
            return;
        }
        /*if(suCache.check(soId + ";" + streamId, su.getLastUpdate())){
            // This SU or a posterior one has already been sent, do not send this one.
            this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "sucache")
            );
            collector.ack(input);
			return;
		}*/

        Set<String> docIds = sou.getSourceIdsByStream(streamId);
        // Remove the origin for which we already have the SU
        docIds.remove(originId);
        // The self last update from current stream
        docIds.add(streamId);

        docs = new HashMap<String, String>();
        try {
            docs.put("$input", suDoc);
            docs.putAll(this.getStreamSUs(docIds, so));
            docs.putAll(this.getGroupSUs(docIds, so));
            docs.put(originId, suDoc);
        } catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            collector.fail(input);
            return;
        }
        // Obtain the highest timestamp from the input docs
        timestamp = su.getLastUpdate();
        for (Map.Entry<String, String> doc : docs.entrySet()) {
            SensorUpdate inputSU;
            try {
                String docContent = doc.getValue();
                if (docContent.equals("null")) {
                    continue;
                }
                inputSU = mapper.readValue(docContent, SensorUpdate.class);
            } catch (Exception e) {
                // TODO Log the error
                e.printStackTrace();
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "error")
                );
                collector.ack(input);
                return;
            }
            timestamp = inputSU.getLastUpdate() > timestamp ? inputSU.getLastUpdate() : timestamp;
        }

        previousSUDoc = docs.get(streamId);
        if (!previousSUDoc.equals("null")) {
            try {
                previousSU = mapper.readValue(previousSUDoc, SensorUpdate.class);
            } catch (Exception e) {
                // TODO Log the error
                e.printStackTrace();
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "error")
                );
                collector.ack(input);
                return;
            }
            // There is already a newer update stored
            if (timestamp <= previousSU.getLastUpdate()) {
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "old")
                );
                collector.ack(input);
                return;
            }
        }
        SensorUpdate resultSU;
        String resultSUDoc;
        try {
            resultSU = sou.getResultSU(streamId, docs, timestamp);
            if (resultSU == null) {
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "null-result")
                );
                collector.ack(input);
                return;
            }
            mapper.setSerializationInclusion(Inclusion.NON_NULL);
            resultSUDoc = mapper.writeValueAsString(resultSU);

        } catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
            collector.ack(input);
            return;
        }
        if(dc.benchmark) {
            String[] fromStr = {so.getId(), streamId};
            resultSU.setStreamsChain(su.getStreamsChain());
            resultSU.setTimestampChain(su.getTimestampChain());
            resultSU.setOriginId(su.getOriginId());

            resultSU.getStreamsChain().add(new ArrayList<String>(Arrays.asList(fromStr)));
            resultSU.getTimestampChain().add(System.currentTimeMillis());
        }

        try {
            resultSUDoc = mapper.writeValueAsString(resultSU);
        } catch (Exception e) {
            // TODO Log the error

            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
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
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "error")
                );
                collector.ack(input);
                return;
			}
		}
		try{
			qc.connect();
			if(!qc.put(dispatcherJson)){
				// TODO Log the error
                System.err.println("Error trying to queue a SU");
                collector.fail(input);
                return;
			}
			qc.disconnect();
		} catch (Exception e) {
			// TODO Log the error
            e.printStackTrace();
			collector.fail(input);
			return;
		}

        // Remove the data that doesn't need to be stored.
        resultSU.setStreamsChain(null);
        resultSU.setTimestampChain(null);
        resultSU.setOriginId(null);

        try {
            resultSUDoc = mapper.writeValueAsString(resultSU);
        } catch (Exception e) {
            // TODO Log the error

            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
            collector.ack(input);
            return;
        }

        try {
            // Send to the API
            restClient.restRequest(
					dc.restBaseURL
							+ "private/" + soId + "/streams/"
							+ streamId + "/" + opid, resultSUDoc,
					RestClient.PUT,
					null);
        } catch(RestClientErrorCodeException e){
            // TODO Log the error
            e.printStackTrace();
            if(e.getRestResponse().getHttpCode()>= 500){
                collector.fail(input);
                return;
            }
            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
            collector.ack(input);
            return;
        }catch (Exception e) {
            // TODO Log the error

            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
            collector.ack(input);
            return;
        }

        //suCache.put(soId+";"+streamId, su.getLastUpdate());
        collector.ack(input);
        return;
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
