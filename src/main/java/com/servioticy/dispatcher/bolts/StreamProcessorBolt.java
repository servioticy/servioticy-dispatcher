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
import com.servioticy.datamodel.reputation.Reputation;
import com.servioticy.datamodel.UpdateDescriptor;
import com.servioticy.datamodel.reputation.ReputationAddress;
import com.servioticy.datamodel.reputation.ReputationAddressSO;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SOGroup;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SOProcessor;
import com.servioticy.dispatcher.SOProcessor010;
import com.servioticy.dispatcher.SUCache;
import com.servioticy.queueclient.KestrelThriftClient;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;
import de.passau.uni.sec.compose.pdp.servioticy.LocalPDP;
import de.passau.uni.sec.compose.pdp.servioticy.PDP;
import de.passau.uni.sec.compose.pdp.servioticy.PermissionCacheObject;
import de.passau.uni.sec.compose.pdp.servioticy.exception.PDPServioticyException;
import de.passau.uni.sec.compose.pdp.servioticy.provenance.ServioticyProvenance;
import com.servioticy.restclient.*;
import org.apache.log4j.Logger;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.RhinoException;

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
    private PDP pdp;
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
        this.pdp = new LocalPDP();
        // Placeholders
        this.pdp.setIdmHost("");
        this.pdp.setIdmPort(0);
        this.pdp.setIdmUser("");
        this.pdp.setIdmPassword("");

        try {
            if (this.qc == null) {
                qc = QueueClient.factory(dc.updatesFeedbackAddress, dc.updatesQueue, "com.servioticy.queueclient.KafkaClient", null);

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

    private Map<String, SensorUpdate> getGroupSUs(Map<String, FutureRestResponse> rrs, SO so, String streamId, Tuple input) throws IOException, RestClientException, RestClientErrorCodeException, ExecutionException, InterruptedException, PDPServioticyException {
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

            PermissionCacheObject pco = new PermissionCacheObject();
            pco.setStream(streamId);
            SensorUpdate lastSU = this.mapper.readValue(rr.getResponse(), SensorUpdate.class);
            pco = this.pdp.checkAuthorization(null, this.mapper.readTree(this.mapper.writeValueAsString(so.getSecurity())), mapper.readTree(mapper.writeValueAsString(lastSU.getSecurity())), pco,
                    PDP.operationID.DispatchData);
            if(!pco.isPermission()){
                ReputationBolt.sendToReputation(collector, mapper, input, lastSU, so, streamId, Reputation.DISCARD_SECURITY, false);
                return null;
            }else {
                groupDocs.put(docId, lastSU);
            }

        }

        return groupDocs;
    }

    private Map<String, FutureRestResponse> getGroupSUAsyncResponses(Set<String>  groupIds, SO so) throws RestClientException, RestClientErrorCodeException, JsonProcessingException {
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

    private FutureRestResponse getGroupSUAsyncResponse(String groupId, SO so) throws RestClientException, RestClientErrorCodeException, JsonProcessingException {
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

    private Map<String, SensorUpdate> getStreamSUs(Map<String, FutureRestResponse> frrs) throws IOException, RestClientException, RestClientErrorCodeException, ExecutionException, InterruptedException {
        Map<String, SensorUpdate> streamDocs = new HashMap<String, SensorUpdate>();

        for(Map.Entry<String, FutureRestResponse> frrEntry: frrs.entrySet()) {
            FutureRestResponse frr = frrEntry.getValue();
            String docId = frrEntry.getKey();
            RestResponse rr;
            streamDocs.put(docId, getStreamSU(frr));
        }
		return streamDocs;
	}

    private Map<String, FutureRestResponse> getStreamSUAsyncResponses(Set<String> streamIds, SO so) throws RestClientException, RestClientErrorCodeException {
        Map<String, FutureRestResponse> rrs = new HashMap();
        Map<String, SensorUpdate> streamDocs = new HashMap<String, SensorUpdate>();
        for(String streamId: streamIds){
            if(!so.getStreams(this.mapper).containsKey(streamId)){
                continue;
            }

            rrs.put(streamId, getStreamSUAsyncResponse(streamId, so));
        }
        return rrs;
    }

    private SensorUpdate getStreamSU(FutureRestResponse frr) throws RestClientErrorCodeException, IOException, RestClientException, ExecutionException, InterruptedException {
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
        return this.mapper.readValue(rr.getResponse(), SensorUpdate.class);
    }

    private FutureRestResponse getStreamSUAsyncResponse(String streamId, SO so) throws RestClientException, RestClientErrorCodeException {
        FutureRestResponse frr;
        frr = restClient.restRequest(
                dc.restBaseURL
                        + "private/" + so.getId() + "/streams/" + streamId + "/lastUpdate",
                null, RestClient.GET,
                null
        );

        return frr;
    }

    public void execute(Tuple input) {
        SensorUpdate su;
        FutureRestResponse previousSURR;
        SensorUpdate previousSU;
        SO so;
        String soId = input.getStringByField("soid");
        String streamId = input.getStringByField("streamid");
        String suDoc = input.getStringByField("su");
        String soDoc = input.getStringByField("so");
        String originId = input.getStringByField("originid");
        SOProcessor sop;
        long timestamp;
        Map<String, SensorUpdate> sensorUpdates;
        Map<String, FutureRestResponse> streamSURRs;
        Map<String, FutureRestResponse> groupSURRs;
        try {
            su = this.mapper.readValue(suDoc, SensorUpdate.class);
            so = this.mapper.readValue(soDoc, SO.class);
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

            previousSU = this.getStreamSU(previousSURR);

            // There is already a newer generated update than the one received
            if (previousSU != null) {
                if (su.getLastUpdate() <= previousSU.getLastUpdate()) {
                    BenchmarkBolt.send(collector, input, dc, suDoc, "old");
                    ReputationBolt.sendToReputation(collector, mapper, input, su, so, streamId, Reputation.DISCARD_TIMESTAMP, true);
                    collector.ack(input);
                    return;
                }
            }
            // At this point we know for sure that at least one input SU is newer

            sensorUpdates = new HashMap<String, SensorUpdate>();
            try {
                sensorUpdates.putAll(this.getStreamSUs(streamSURRs));
                sensorUpdates.put(streamId, previousSU);
                Map<String, SensorUpdate> groupLastSus = this.getGroupSUs(groupSURRs, so, streamId, input);
                if (groupLastSus == null) {
                    LOG.info(soId + ":" + streamId + " (" + originId +
                            ") doesn't have permissions to read one or more sources. Discarding composition.");
                    collector.ack(input);
                    return;
                }
                sensorUpdates.putAll(groupLastSus);
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
                resultSU = sop.getResultSU(streamId, sensorUpdates, originId, soId, timestamp);
                // Reputation needs the previousSU away
                if(originId != streamId){
                    sensorUpdates.remove(streamId);
                }
                if (resultSU == null) {
                    LOG.info(soId + ":" + streamId + " (" + originId + ") composition filtered");
                    BenchmarkBolt.send(collector, input, dc, suDoc, "filtered");
                    ReputationBolt.sendAllToReputation(collector, mapper, input, sensorUpdates, originId, so, streamId, Reputation.DISCARD_FILTER);
                    collector.ack(input);
                    return;
                }
                resultSUDoc = this.mapper.writeValueAsString(resultSU);

            } catch (RhinoException e) {
                LOG.warn(soId + ":" + streamId + " (" + originId + ") error executing JS code", e);
                BenchmarkBolt.send(collector, input, dc, suDoc, "script-error");
                //Reputation
                // Reputation needs the previousSU away
                if(originId != streamId){
                    sensorUpdates.remove(streamId);
                }
                ReputationBolt.sendAllToReputation(collector, mapper, input, sensorUpdates, originId, so, streamId, Reputation.DISCARD_ERROR);
                collector.ack(input);
                return;
            }


            // The output update descriptor
            UpdateDescriptor ud = new UpdateDescriptor();
            ud.setSoid(soId);
            ud.setStreamid(streamId);
            ud.setSu(resultSU);
            String upDescriptorDoc = this.mapper.writeValueAsString(ud);

            // Remove the data that doesn't need to be stored.
            resultSU.setTriggerPath(null);
            resultSU.setPathTimestamps(null);
            resultSU.setOriginId(null);

            resultSUDoc = this.mapper.writeValueAsString(resultSU);

            // Send to the API
            restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + soId + "/streams/"
                            + streamId, resultSUDoc,
                    RestClient.PUT,
                    null);

            if(dc.benchmark) {
                String[] fromStr = {so.getId(), streamId};
                resultSU.setTriggerPath(su.getTriggerPath());
                resultSU.setPathTimestamps(su.getPathTimestamps());
                resultSU.setOriginId(su.getOriginId());

                resultSU.getTriggerPath().add(new ArrayList<String>(Arrays.asList(fromStr)));
                resultSU.getPathTimestamps().add(System.currentTimeMillis());
            }
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

            ReputationBolt.sendAllToReputation(collector, mapper, input, sensorUpdates, originId, so, streamId, Reputation.DISCARD_NONE);
        } catch(RestClientErrorCodeException e) {
            if (e.getRestResponse().getHttpCode() >= 500) {
                LOG.error(soId + ":" + streamId + " (" + originId + ") error on the API with code "
                        + e.getRestResponse().getHttpCode() + ": " +
                        e.getRestResponse().getResponse(), e);
                collector.fail(input);
                return;
            }
            LOG.error(soId + ":" + streamId + " (" + originId + ") error returned by the API with code "
                    + e.getRestResponse().getHttpCode() + ": " +
                                e.getRestResponse().getResponse(), e);
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        } catch (Exception e) {
            LOG.warn(soId + ":" + streamId + " (" + originId + ") composition error. JSON malformed?" + suDoc, e);
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        }

        //suCache.put(soId+";"+streamId, su.getLastUpdate());
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
        declarer.declareStream(Reputation.STREAM_SO_SO, new Fields("in-soid", "in-streamid", "out-soid", "out-streamid", "user_timestamp", "date", "event", "discard"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
