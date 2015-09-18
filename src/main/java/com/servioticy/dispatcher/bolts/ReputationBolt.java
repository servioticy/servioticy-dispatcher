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
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.reputation.*;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.dispatcher.DispatcherContext;
import de.passau.uni.sec.compose.pdp.servioticy.exception.PDPServioticyException;
import de.passau.uni.sec.compose.pdp.servioticy.provenance.ServioticyProvenance;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 */
public class ReputationBolt implements IRichBolt{
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private TopologyContext context;
    private DispatcherContext dc;
    private Map<String, Integer> mapBoltStream;
    private Cluster cbCluster;
    private Bucket reputationBucket;
    ObjectMapper mapper;

    private static final int STREAM_SO_PUBSUB = 0;
    private static final int STREAM_SO_SERVICE = 1;
    private static final int STREAM_SO_SO = 2;
    private static final int STREAM_SO_USER = 3;
    private static final int STREAM_WO_SO = 4;

    public ReputationBolt(DispatcherContext dc) {
        this.dc = dc;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        mapper = new ObjectMapper();
        this.collector = collector;
        this.context = context;
        this.mapBoltStream = new HashMap<String, Integer>();
        this.mapBoltStream.put(Reputation.STREAM_SO_PUBSUB, ReputationBolt.STREAM_SO_PUBSUB);
        this.mapBoltStream.put(Reputation.STREAM_SO_SERVICE, ReputationBolt.STREAM_SO_SERVICE);
        this.mapBoltStream.put(Reputation.STREAM_SO_SO, ReputationBolt.STREAM_SO_SO);
        this.mapBoltStream.put("default", ReputationBolt.STREAM_SO_USER);
        this.mapBoltStream.put(Reputation.STREAM_WO_SO, ReputationBolt.STREAM_WO_SO);
        List<String> nodes = new ArrayList<String>();
        for(String address: dc.storageAddresses){
            nodes.add(address);
        }

        cbCluster = CouchbaseCluster.create(nodes);
        reputationBucket = cbCluster.openBucket(dc.reputationBucket);

    }

    public static void sendAllToReputation(OutputCollector collector, ObjectMapper mapper, Tuple input, Map<String, SensorUpdate> sensorUpdates, String originId, SO so, String streamId, String reason) throws IOException, PDPServioticyException {
        for (Map.Entry<String, SensorUpdate> entry: sensorUpdates.entrySet()) {
            boolean event = entry.getKey().equals(originId);
            SensorUpdate entrySU = entry.getValue();
            if(entrySU == null){
                continue;
            }
            sendToReputation(collector, mapper, input, entrySU, so, streamId, reason, event);
        }
    }

    public static void sendToReputation(OutputCollector collector, ObjectMapper mapper, Tuple input, SensorUpdate su, SO so, String streamId, String reason, Boolean event) throws IOException, PDPServioticyException {
        ServioticyProvenance prov = new ServioticyProvenance();
        ReputationAddressSO src = mapper.readValue(
                prov.getSourceFromSecurityMetaDataAsString(
                        mapper.writeValueAsString(
                                su.getSecurity()
                        )
                ), ReputationAddressSO.class);
        collector.emit(Reputation.STREAM_SO_SO, input,
                new Values(src.getSoid(), // in-soid
                        src.getStreamid(), // in-streamid
                        so.getId(),
                        streamId,
                        su.getLastUpdate(),
                        System.currentTimeMillis(),
                        event,
                        reason)
        );
    }

    @Override
    public void execute(Tuple input) {
        Reputation reputation = new Reputation();
        reputation.setEvent(true);
//        reputation.setFresh(true);
        reputation.setDiscard(Reputation.DISCARD_NONE);
        reputation.setUserTimestamp(input.getLongByField("user_timestamp") == null ? System.currentTimeMillis():input.getLongByField("user_timestamp"));
        reputation.setDate(input.getLongByField("date"));
        reputation.setInternalPublisher(null);
        ReputationAddressSO soInAddress;
        ReputationAddressSO soOutAddress;
        ReputationAddressPubSub psOutAddress;
        ReputationAddressUser userOutAddress;
        ReputationAddressWebObject webObjectAddress;

        // src
        String stormStream = input.getSourceStreamId();
        switch (this.mapBoltStream.get(stormStream)){
            case ReputationBolt.STREAM_WO_SO:
                webObjectAddress = new ReputationAddressWebObject();
                webObjectAddress.setWebobject(true);
                reputation.setSrc(webObjectAddress);
                break;
            case ReputationBolt.STREAM_SO_SO:
            case ReputationBolt.STREAM_SO_PUBSUB:
            case ReputationBolt.STREAM_SO_USER:
            case ReputationBolt.STREAM_SO_SERVICE:
                soInAddress = new ReputationAddressSO();
                soInAddress.setSoid(input.getStringByField("in-soid"));
                soInAddress.setStreamid(input.getStringByField("in-streamid"));
                reputation.setSrc(soInAddress);
                break;
            default:
                break;
        }

        // dest
        switch (this.mapBoltStream.get(input.getSourceStreamId())){
            case ReputationBolt.STREAM_WO_SO:
            case ReputationBolt.STREAM_SO_SO:
                soOutAddress = new ReputationAddressSO();
                soOutAddress.setSoid(input.getStringByField("out-soid"));
                soOutAddress.setStreamid(input.getStringByField("out-streamid"));
                reputation.setDest(soOutAddress);
                break;
            case ReputationBolt.STREAM_SO_PUBSUB:
                psOutAddress = new ReputationAddressPubSub();
                psOutAddress.setPubSubTopic(input.getStringByField("out-topic"));
                psOutAddress.setOnBehalfOf(input.getStringByField("out-user_id"));
                reputation.setDest(psOutAddress);
                reputation.setInternalPublisher(false);
            case ReputationBolt.STREAM_SO_SERVICE:
                reputation.setInternalPublisher(true);
                break;
            case ReputationBolt.STREAM_SO_USER:
                userOutAddress = new ReputationAddressUser();
                userOutAddress.setUserId(input.getStringByField("out-user_id"));
                reputation.setDest(userOutAddress);
                break;
            default:
                break;
        }

        // others
        switch (this.mapBoltStream.get(input.getSourceStreamId())){
            case ReputationBolt.STREAM_SO_SO:
                reputation.setEvent(input.getBooleanByField("event"));
                reputation.setDiscard(input.getStringByField("discard"));
                break;
            case ReputationBolt.STREAM_WO_SO:
//                reputation.setFresh(input.getBooleanByField("fresh"));
                break;
            case ReputationBolt.STREAM_SO_USER:
                reputation.setEvent(false);
                break;
            default:
                break;
        }
        try {
            String repString = mapper.writeValueAsString(reputation);
            reputationBucket.insert(JsonDocument.create(
                    Long.toHexString(UUID.randomUUID().getMostSignificantBits()) + "-" + reputation.getDate(),
                    2*24*3600,
                    JsonObject.fromJson(repString)));
        } catch (Exception e) {
            // TODO log the error
            e.printStackTrace();
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
