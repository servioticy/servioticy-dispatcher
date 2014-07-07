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
import com.servioticy.datamodel.SOSubscription;
import com.servioticy.datamodel.SensorUpdate;
import com.servioticy.dispatcher.DispatcherContext;
import com.servioticy.dispatcher.SOUtils;
import com.servioticy.restclient.RestClient;
import com.servioticy.restclient.RestClientErrorCodeException;
import com.servioticy.restclient.RestResponse;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
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
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "error")
                );
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
					dc.restBaseURL
						+ "private/" + destination, null, RestClient.GET,
						null);
			soDoc = rr.getResponse();
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
            e.printStackTrace();
            collector.ack(input);
			return;
        }catch (Exception e) {
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
		try{
			so = mapper.readValue(soDoc,
					SO.class);
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
			SOUtils sou = new SOUtils(mapper.readValue(soDoc, SO.class));

//            SensorUpdate su = mapper.readValue(suDoc, SensorUpdate.class);
            boolean emitted = false;
            for (String streamIdByDoc : sou.getStreamsBySourceId(docId)) {
                // If the SU comes from the same stream than it is going, it must be stopped
//                boolean beenThere = false;
//                for (ArrayList<String> prevStream : su.getStreamsChain()) {
//                    beenThere = (destination == prevStream.get(0) && streamIdByDoc == prevStream.get(1));
//                    if (beenThere) break;
//                }
//
//                if (beenThere) {
//                    continue;
//                }
                this.collector.emit("default", input,
                        new Values(destination,
                                streamIdByDoc,
													soDoc,
													docId,
													suDoc));
                emitted = true;
            }
            if (!emitted) {
                if (dc.benchmark) this.collector.emit("benchmark", input,
                        new Values(suDoc,
                                System.currentTimeMillis(),
                                "no-stream")
                );
            }
        } catch (Exception e) {
            if (dc.benchmark) this.collector.emit("benchmark", input,
                    new Values(suDoc,
                            System.currentTimeMillis(),
                            "error")
            );
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
        declarer.declareStream("default", new Fields("soid", "streamid", "so", "originid", "su"));
        if (dc.benchmark) declarer.declareStream("benchmark", new Fields("su", "stopts", "reason"));
    }

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
