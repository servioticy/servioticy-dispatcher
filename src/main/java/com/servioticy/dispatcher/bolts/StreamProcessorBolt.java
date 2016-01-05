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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.UpdateDescriptor;
import com.servioticy.datamodel.reputation.Discard;
import com.servioticy.datamodel.reputation.OnBehalf;
import com.servioticy.datamodel.reputation.Reputation;
import com.servioticy.datamodel.sensorupdate.ProvenanceUnit;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SOGroup;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.*;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;
import com.servioticy.restclient.*;
import org.apache.log4j.Logger;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class StreamProcessorBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TopologyContext context;
	private SUCache suCache;
	private QueueClient qc;
	private RestClient restClient;
    private DispatcherContext dc;
    private ObjectMapper mapper;
    private static Logger LOG = org.apache.log4j.Logger.getLogger(StreamProcessorBolt.class);


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
        this.qc = null;

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
        this.mapper = new ObjectMapper();
        try {
            if (this.qc == null) {
                qc = QueueClient.factory(dc.updatesFeedbackAddress, dc.updatesQueue,
                        "com.servioticy.queueclient.KafkaClient", null);

            }
            qc.connect();
        } catch(Exception e){
            // TODO log error
            e.printStackTrace();
        }
		if(restClient == null){
			restClient = new RestClient();
		}
	}

    private Map<String, SensorUpdate> getGroupSUs(Map<String, FutureRestResponse> rrs, Reputation reputation,
                                                  SO so, Tuple input)
            throws IOException, RestClientException, RestClientErrorCodeException, ExecutionException,
            InterruptedException {
		Map<String, SensorUpdate> groupDocs = new HashMap<String, SensorUpdate>();

        for(Map.Entry<String, FutureRestResponse> frrEntry: rrs.entrySet()){
            FutureRestResponse frr = frrEntry.getValue();
            String docId = frrEntry.getKey();
            RestResponse rr;
            try {
                rr = frr.get();
            } catch (RestClientErrorCodeException e) {
                if (e.getRestResponse().getHttpCode() >= 500) {
                    throw e;
                }
                groupDocs.put(docId, null);
                continue;
            }
            // In case there is no update.
            if(rr.getHttpCode() == 204){
                groupDocs.put(docId, null);
                continue;
            }

            SensorUpdate su = this.mapper.readValue(rr.getResponse(), SensorUpdate.class);
            reputation.setSoId(su.getSoId());
            reputation.setStreamId(su.getStreamId());
            reputation.setSuId(su.getId());

            if(!Auth.check(so, su)){
                reputation.setDiscard(new Discard());
                reputation.getDiscard().setReason(Discard.REASON_NO_AUTH);
                reputation.getDiscard().setMessage("No authorization to use SU '" + su.getId() + "' on SO '" +
                        so.getId() + "'");
                collector.emit("reputation", input,
                    new Values(mapper.writeValueAsString(reputation))
                );
                return null;
            }
            reputation.setSoId(su.getSoId());
            reputation.setStreamId(su.getStreamId());
            reputation.setSuId(su.getId());
            collector.emit("reputation", input,
                    new Values(mapper.writeValueAsString(reputation))
            );

			groupDocs.put(docId, su);
			
		}
		
		return groupDocs;
	}

    private Map<String, FutureRestResponse> getGroupSUAsyncResponses(Set<String>  groupIds, SO so)
            throws RestClientException, RestClientErrorCodeException, JsonProcessingException {
        Map<String, FutureRestResponse> rrs = new HashMap();
        if(so.getGroups() == null){
            return rrs;
        }
        for(String groupId: groupIds){
            if(!so.getGroups().containsKey(groupId)){
                continue;
            }

            rrs.put(groupId, getGroupSUAsyncResponse(groupId, so));
        }
        return rrs;
    }

    private FutureRestResponse getGroupSUAsyncResponse(String groupId, SO so)
            throws RestClientException, RestClientErrorCodeException, JsonProcessingException {
        FutureRestResponse frr;

        SOGroup group = so.getGroups().get(groupId);
        // TODO Resolve dynsets
        String glurstr = this.mapper.writeValueAsString(group);

        frr = restClient.restRequest(
                dc.restBaseURL + "private/groups/lastUpdate",
                glurstr, RestClient.POST,
                null
        );

        return frr;
    }

    private Map<String, SensorUpdate> getStreamSUs(Map<String, FutureRestResponse> frrs, Reputation reputation,
                                                   Tuple input)
            throws IOException, RestClientException, RestClientErrorCodeException, ExecutionException,
            InterruptedException {
        Map<String, SensorUpdate> streamDocs = new HashMap<String, SensorUpdate>();

        for(Map.Entry<String, FutureRestResponse> frrEntry: frrs.entrySet()) {
            FutureRestResponse frr = frrEntry.getValue();
            String docId = frrEntry.getKey();
            RestResponse rr;
            streamDocs.put(docId, getStreamSU(frr, reputation, input));
        }
		return streamDocs;
	}

    private Map<String, FutureRestResponse> getStreamSUAsyncResponses(Set<String> streamIds, SO so) throws
            RestClientException, RestClientErrorCodeException {
        Map<String, FutureRestResponse> rrs = new HashMap();
        for(String streamId: streamIds){
            if(!so.getStreams(this.mapper).containsKey(streamId)){
                continue;
            }

            rrs.put(streamId, getStreamSUAsyncResponse(streamId, so));
        }
        return rrs;
    }

    private SensorUpdate getStreamSU(FutureRestResponse frr, Reputation reputation, Tuple input)
            throws RestClientErrorCodeException, IOException, RestClientException, ExecutionException,
            InterruptedException {
        RestResponse rr;
        try {
            rr = frr.get();
        } catch (RestClientErrorCodeException e) {
            if (e.getRestResponse().getHttpCode() >= 500) {
                throw e;
            }
            return null;
        }
        // In case there is no update.
        if(rr.getHttpCode() == 204){
            return null;
        }
        SensorUpdate su = this.mapper.readValue(rr.getResponse(), SensorUpdate.class);
        if(reputation != null) {
            reputation.setSoId(su.getSoId());
            reputation.setStreamId(su.getStreamId());
            reputation.setSuId(su.getId());

            collector.emit("reputation", input, new Values(mapper.writeValueAsString(reputation)));
        }

        return su;
    }

    private FutureRestResponse getStreamSUAsyncResponse(String streamId, SO so)
            throws RestClientException, RestClientErrorCodeException {
        FutureRestResponse frr;
        frr = restClient.restRequest(
                dc.restBaseURL
                        + "private/" + so.getId() + "/streams/" + streamId + "/lastUpdate",
                null, RestClient.GET,
                null
        );

        return frr;
    }

    private ArrayList<ArrayList<ProvenanceUnit>> mergeProvenance(SensorUpdate trigger, List<SensorUpdate> reads,
                                                                 ProvenanceUnit pu){
        ArrayList<ProvenanceUnit> provPath;
        ArrayList<SensorUpdate> sus = new ArrayList<SensorUpdate>();
        ArrayList<ArrayList<ProvenanceUnit>> provenance = new ArrayList<ArrayList<ProvenanceUnit>>();

        sus.add(trigger);
        sus.addAll(reads);

        for(SensorUpdate su: sus){
            ArrayList<ArrayList<ProvenanceUnit>> suProvenance = su.getProvenance();
            if(suProvenance==null) continue;
            for(int i = 0; i < suProvenance.size(); i++){
                provPath = new ArrayList<ProvenanceUnit>();
                if(i == 0) provPath.add(pu);
                provPath.addAll(suProvenance.get(i));
                provenance.add(provPath);
            }
        }
        return provenance;
    }

    public void execute(Tuple input) {
        SensorUpdate su;
        FutureRestResponse previousSURR;
        SensorUpdate previousSU;
        SO so;
        Reputation writeReputation = new Reputation();
        String soId = input.getStringByField("soid");
        String streamId = input.getStringByField("streamid");
        String suDoc = input.getStringByField("su");
        String soDoc = input.getStringByField("so");
        String writeRepDoc = input.getStringByField("reputation");
        String originId = input.getStringByField("originid");
        SOProcessor sop;
        long timestamp;
        Map<String, SensorUpdate> sensorUpdates;
        Map<String, FutureRestResponse> streamSURRs;
        Map<String, FutureRestResponse> groupSURRs;

        try {
            su = this.mapper.readValue(suDoc, SensorUpdate.class);
            so = this.mapper.readValue(soDoc, SO.class);
            writeReputation = mapper.readValue(writeRepDoc, Reputation.class);

            sop = SOProcessor.factory(so, this.mapper);

            // Begin all HTTP requests
            previousSURR = this.getStreamSUAsyncResponse(streamId, so);

            if(sop.getClass() == SOProcessor010.class) {
                // It is not needed to replace the alias, it has been already done in the previous bolt.
                ((SOProcessor010)sop).compileJSONPaths();
            }
            Set<String> docIds = sop.getSourceIdsByStream(streamId);
            // Remove the origin for which we already have the SU
            docIds.remove(originId);

            docIds.add(streamId);

            streamSURRs = this.getStreamSUAsyncResponses(docIds, so);
            groupSURRs = this.getGroupSUAsyncResponses(docIds, so);

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

            // Get the last update from the current stream

            previousSU = this.getStreamSU(previousSURR, null, null);

            // There is already a newer generated update than the one received
            if (previousSU != null) {
                if (su.getLastUpdate() <= previousSU.getLastUpdate() ) {
                    BenchmarkBolt.send(collector, input, dc, suDoc, "old");
                    String logMsg = "There is already a newer generated update than the one received: " +
                            previousSU.getLastUpdate() + " >= " + su.getLastUpdate();
                    LOG.info(logMsg);
                    writeReputation.setDiscard(new Discard());
                    writeReputation.getDiscard().setReason(Discard.REASON_OLD_TRIGGER);
                    writeReputation.getDiscard().setMessage(logMsg);
                    collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));
                    collector.ack(input);
                    return;
                }
            }
            // At this point we know for sure that at least one input SU is newer

            sensorUpdates = new HashMap<String, SensorUpdate>();

            Map<String, SensorUpdate> readSUs;
            try {
                Reputation readReputation = new Reputation();
                readReputation.setOnBehalf(new OnBehalf());
                readReputation.getOnBehalf().setSoId(soId);
                readReputation.getOnBehalf().setStreamId(streamId);
                readReputation.getOnBehalf().setSuId(su.getId());
                // TODO readReputation.getOnBehalf().setUserid(so.getUserId());

                Map<String, SensorUpdate> streamSUs = this.getStreamSUs(streamSURRs, readReputation, input);
                Map<String, SensorUpdate> groupSUs = this.getGroupSUs(groupSURRs, readReputation, so, input);
                if (groupSUs == null) {
                    LOG.info(soId + ":" + streamId + " (" + originId +
                            ") doesn't have permissions to read one or more sources. Discarding composition.");
                    collector.ack(input);
                    return;
                }
                readSUs = new HashMap<String, SensorUpdate>();
                readSUs.putAll(streamSUs);
                readSUs.putAll(groupSUs);
                sensorUpdates.putAll(streamSUs);
                sensorUpdates.put(streamId, previousSU);
                sensorUpdates.putAll(groupSUs);
                sensorUpdates.put(originId, su);
            } catch (Exception e) {
                LOG.warn(soId + ":" + streamId + " (" + originId + ") error obtaining sources", e);
                collector.fail(input);
                return;
            }

            // Obtain the highest timestamp from the input docs
            timestamp = su.getLastUpdate();
            for (Map.Entry<String, SensorUpdate> doc : sensorUpdates.entrySet()) {
                SensorUpdate inputSU;
                inputSU = doc.getValue();
                if (inputSU == null) {
                    continue;
                }
                timestamp = inputSU.getLastUpdate() > timestamp ? inputSU.getLastUpdate() : timestamp;
            }
            SensorUpdate resultSU;
            String resultSUDoc;
            try {
                resultSU = sop.getResultSU(streamId, sensorUpdates, originId, timestamp);
                if (resultSU == null) {
                    String logMsg = soId + ":" + streamId + " (" + originId + ") composition filtered";
                    LOG.info(logMsg);
                    BenchmarkBolt.send(collector, input, dc, suDoc, "filtered");
                    writeReputation.setDiscard(new Discard());
                    writeReputation.getDiscard().setReason(Discard.REASON_FILTERED);
                    writeReputation.getDiscard().setMessage(logMsg);
                    collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));
                    collector.ack(input);
                    return;
                }
                // TODO The suId generation should be a shared method with the API
                ArrayList<ArrayList<ProvenanceUnit>> provenance = this.mergeProvenance(su,
                        new ArrayList<SensorUpdate>(readSUs.values()),
                            new ProvenanceUnit(soId, streamId, soId + "-" + streamId + "-" + resultSU.getLastUpdate(),
                                    System.currentTimeMillis()));
                resultSU.setProvenance(provenance);
                resultSU.setOriginId(null);
                resultSUDoc = this.mapper.writeValueAsString(resultSU);

            } catch (ScriptException e) {
                String logMsg = soId + ":" + streamId + " (" + originId + ") error executing JS code";
                LOG.warn(logMsg, e);
                writeReputation.setDiscard(new Discard());
                writeReputation.getDiscard().setReason(Discard.REASON_SCRIPT_ERROR);
                writeReputation.getDiscard().setMessage(logMsg);
                collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));
                BenchmarkBolt.send(collector, input, dc, suDoc, "script-error");
                collector.ack(input);
                return;
            }

            // Send to the API
            restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + soId + "/streams/"
                            + streamId, resultSUDoc,
                    RestClient.PUT,
                    null);
            if(dc.benchmark){
                resultSU.setOriginId(su.getOriginId());
            }
            writeReputation.setSuId(su.getId());
            // Send to reputation
            collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));

            // The output update descriptor
            UpdateDescriptor ud = new UpdateDescriptor();
            ud.setSoid(soId);
            ud.setStreamid(streamId);
            ud.setSu(resultSU);
            String upDescriptorDoc = this.mapper.writeValueAsString(ud);

            // Put to the queue
            try{
                if(!qc.isConnected()) {
                    qc.connect();
                }

                if(!qc.put(upDescriptorDoc)){
                    LOG.warn(soId + ":" + streamId + " (" + originId + ") error sending feedback");
                    BenchmarkBolt.send(collector, input, dc, suDoc, "script-error");
                    collector.fail(input);
                    return;
                }
                qc.disconnect();
            } catch (Exception e) {
                LOG.warn(soId + ":" + streamId + " (" + originId + ") error sending feedback", e);
                collector.fail(input);
                return;
            }

        } catch(RestClientErrorCodeException e) {
            if (e.getRestResponse().getHttpCode() >= 500) {
                LOG.error(soId + ":" + streamId + " (" + originId + ") error on the API with code "
                        + e.getRestResponse().getHttpCode() + ": " +
                        e.getRestResponse().getResponse(), e);
                collector.fail(input);
                return;
            }
            String logMsg =  soId + ":" + streamId + " (" + originId + ") error returned by the API with code "
                    + e.getRestResponse().getHttpCode() + ": " +
                    e.getRestResponse().getResponse();
            LOG.error(logMsg, e);
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            writeReputation.setDiscard(new Discard());
            writeReputation.getDiscard().setReason(Discard.REASON_ACCESSIBILITY);
            writeReputation.getDiscard().setMessage(logMsg);
            try {
                collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));
            } catch (JsonProcessingException e1) {}
            collector.ack(input);
            return;
        } catch (Exception e) {
            String logMsg = soId + ":" + streamId + " (" + originId + ") composition error. JSON malformed?" + suDoc;
            LOG.warn(logMsg, e);
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            writeReputation.setDiscard(new Discard());
            writeReputation.getDiscard().setReason(Discard.REASON_COMPOSITION_ERROR);
            writeReputation.getDiscard().setMessage(logMsg);
            try {
                collector.emit("reputation", input, new Values(mapper.writeValueAsString(writeReputation)));
            } catch (JsonProcessingException e1) {}
            collector.ack(input);
            return;
        }

        collector.ack(input);
        return;
    }

	public void cleanup() {
        try {
            this.qc.disconnect();
        } catch (QueueClientException e) {
            // TODO log error
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));
        declarer.declareStream("reputation", new Fields("reputation"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
