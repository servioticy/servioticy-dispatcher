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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.reputation.Discard;
import com.servioticy.datamodel.reputation.OnBehalf;
import com.servioticy.datamodel.reputation.Reputation;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SOGroup;
import com.servioticy.datamodel.subscription.SOSubscription;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.Auth;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SOProcessor;
import com.servioticy.dispatcher.SOProcessor010;
import com.servioticy.restclient.FutureRestResponse;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestResponse;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;

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
    private DispatcherContext dc;
    private ObjectMapper mapper;
    private static Logger LOG = org.apache.log4j.Logger.getLogger(StreamDispatcherBolt.class);


    public StreamDispatcherBolt(DispatcherContext dc){
        this.dc = dc;
    }

    // For testing purposes
    public StreamDispatcherBolt(DispatcherContext dc, RestClient restClient){
        this.dc = dc;
        this.restClient = restClient;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.mapper = new ObjectMapper();
        this.collector = collector;
        this.context = context;
        if(restClient == null){
            restClient = new RestClient();
        }
    }

    public void execute(Tuple input) {
        SOSubscription soSub = null;
        SO so;
        RestResponse rr;
        FutureRestResponse frr;

        String suDoc = input.getStringByField("su");
        String docId = input.getStringByField("docid");
        String destination = input.getStringByField("destination");

        String soDoc;

        try{
            frr = restClient.restRequest(
                    dc.restBaseURL
                            + "private/" + destination, null, RestClient.GET,
                    null);
            rr = frr.get();
            soDoc = rr.getResponse();
            so = this.mapper.readValue(soDoc,
                    SO.class);

            if(input.getSourceStreamId().equals("stream")){
                if(so.getGroups() != null){
                    for(Entry<String, SOGroup> group: so.getGroups().entrySet()){
                        // If there is a group called like the stream, then the docname refers to the group.
                        if(group.getKey().equals(docId)){
                            collector.ack(input);
                            return;
                        }
                    }
                }
            }

            SensorUpdate su = this.mapper.readValue(suDoc, SensorUpdate.class);

            SOProcessor sop = SOProcessor.factory(so, this.mapper);
            if(sop.getClass() == SOProcessor010.class) {
                soDoc = ((SOProcessor010)sop).replaceAliases();
                ((SOProcessor010)sop).compileJSONPaths();
            }

            boolean emitted = false;
            boolean authorized = Auth.check(so, su);
            Reputation reputation = new Reputation();
            // TODO reputation.setOwnerId(so.getOwnerId())
            reputation.setSoId(destination);
            reputation.setAction(Reputation.ACTION_WRITE);
            reputation.setOnBehalf(new OnBehalf());
            reputation.getOnBehalf().setType(OnBehalf.TYPE_STREAM);
            reputation.getOnBehalf().setSoId(su.getSoId());
            reputation.getOnBehalf().setStreamId(su.getStreamId());
            // TODO reputation.getOnBehalf().setUserId(GET_SO(su.getSoId()).getOwnerId())
            reputation.getOnBehalf().setSuId(su.getId());

            for (String streamIdByDoc : sop.getStreamsBySourceId(docId)) {
                // If the SU comes from the same stream than it is going, it must be stopped
//                boolean beenThere = false;
//                for (ArrayList<String> prevStream : su.getTriggerPath()) {
//                    beenThere = (destination == prevStream.get(0) && streamIdByDoc == prevStream.get(1));
//                    if (beenThere) break;
//                }
//
//                if (beenThere) {
//                    continue;
//                }
                reputation.setStreamId(streamIdByDoc);
                if(!authorized){

                    reputation.setDiscard(new Discard());
                    reputation.getDiscard().setReason(Discard.REASON_NO_AUTH);
                    reputation.getDiscard().setMessage("No authorization to use SU '" + su.getId() + "' on SO '" +
                            so.getId() + "'");
                    collector.emit("reputation", input,
                            new Values(mapper.writeValueAsString(reputation))
                    );
                    continue;
                }
                this.collector.emit("default", input,
                        new Values(destination,
                                streamIdByDoc,
                                soDoc,
                                docId,
                                suDoc,
                                mapper.writeValueAsString(reputation)));
                emitted = true;
            }
            if (!emitted) {
                BenchmarkBolt.send(collector, input, dc, suDoc, "no-stream");
                // TODO Reputation if the group *and* the stream does not exist.
            }
        } catch(RestClientErrorCodeException e){
            if(e.getRestResponse().getHttpCode()>= 500){
                LOG.error(destination + " ("+ input.getStringByField("originso") + "->" + docId + ") retrieving SO from the API failed", e);
                collector.fail(input);
                return;
            }
            LOG.warn(destination + " ("+ input.getStringByField("originso") + "->" + docId + ") SO was not found by the API", e);
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        }catch (Exception e) {
            // TODO Log the error
            e.printStackTrace();
            BenchmarkBolt.send(collector, input, dc, suDoc, "error");
            collector.ack(input);
            return;
        }
        collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("default", new Fields("soid", "streamid", "so", "originid", "su", "reputation"));
        declarer.declareStream("reputation", new Fields("reputation"));
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}