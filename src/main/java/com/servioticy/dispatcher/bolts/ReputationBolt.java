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
import com.couchbase.client.CouchbaseClient;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.reputation.*;
import com.servioticy.dispatcher.DispatcherContext;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
    private CouchbaseClient cbClient;
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
        this.mapBoltStream.put(Reputation.STREAM_SO_USER, ReputationBolt.STREAM_SO_USER);
        this.mapBoltStream.put(Reputation.STREAM_WO_SO, ReputationBolt.STREAM_WO_SO);
        ArrayList<URI> nodes = new ArrayList<URI>();
        nodes.add(URI.create("http://192.168.56.101:8091/pools"));

        try {
            cbClient = new CouchbaseClient(nodes, "reputation", "");
        } catch (IOException e) {
            throw new RuntimeException();
        }

    }

    @Override
    public void execute(Tuple input) {
        Reputation reputation = new Reputation();
        reputation.setEvent(true);
        reputation.setFresh(true);
        reputation.setDiscard(Reputation.DISCARD_NONE);
        reputation.setUserTimestamp(input.getLongByField("user_timestamp"));
        reputation.setDate(input.getLongByField("date"));
        ReputationAddressSO soInAddress;
        ReputationAddressSO soOutAddress;
        ReputationAddressPubSub psOutAddress;
        ReputationAddressUser userOutAddress;
        ReputationAddressService servOutAddress;

        // src
        switch (this.mapBoltStream.get(input.getSourceStreamId())){
            case ReputationBolt.STREAM_WO_SO:
                reputation.setSrc("WEB_OBJECT");
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
                psOutAddress.setPubSubTopic(input.getStringByField("topic"));
                reputation.setDest(psOutAddress);
                break;
            case ReputationBolt.STREAM_SO_USER:
                userOutAddress = new ReputationAddressUser();
                userOutAddress.setUserId(input.getStringByField("user_id"));
                reputation.setDest(userOutAddress);
                break;
            case ReputationBolt.STREAM_SO_SERVICE:
                servOutAddress = new ReputationAddressService();
                servOutAddress.setOnBehalfOf(input.getStringByField("user_id"));
                servOutAddress.setServiceId(input.getStringByField("service_id"));
                reputation.setDest(servOutAddress);
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
            case ReputationBolt.STREAM_SO_USER:
                reputation.setEvent(false);
                break;
            case ReputationBolt.STREAM_WO_SO:
                reputation.setFresh(input.getBooleanByField("fresh"));
            default:
                break;
        }
        try {
            String repString = mapper.writeValueAsString(reputation);
            cbClient.set(
                    Long.toHexString(UUID.randomUUID().getMostSignificantBits()) + "-" + reputation.getDate(),
                    repString
            ).get();
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
