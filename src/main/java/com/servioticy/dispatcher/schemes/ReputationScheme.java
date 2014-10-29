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
package com.servioticy.dispatcher.schemes;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.UpdateDescriptor;
import com.servioticy.datamodel.reputation.ReputationSOUserDescriptor;

import java.util.List;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class ReputationScheme implements Scheme {

	public List<Object> deserialize(byte[] bytes) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			String inputDoc = new String(bytes, "UTF-8");
			ReputationSOUserDescriptor rd = mapper.readValue(inputDoc, ReputationSOUserDescriptor.class);
			return new Values(rd.getSrc().getSoid(), rd.getSrc().getStreamid(), rd.getDest().getUserId(), rd.getSu().getLastUpdate(), System.currentTimeMillis());
		} catch(Exception e){
			// TODO Log the error
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		return new Fields("in-soid", "in-streamid", "out-user_id", "user_timestamp", "date");
	}

}
