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

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.actuation.ActuationDescriptor;
import org.apache.log4j.Logger;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class ActuationScheme implements Scheme {
	private static Logger LOG = org.apache.log4j.Logger.getLogger(ActuationScheme.class);
	private ObjectMapper mapper;
	public ActuationScheme(){
		this.mapper = new ObjectMapper();
	}

	public List<Object> deserialize(byte[] bytes) {
		String inputDoc = "";
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInput in = null;
			in = new ObjectInputStream(bis);
			Object o = in.readObject();
			bis.close();
			inputDoc = (String) o;
			ActuationDescriptor ad = this.mapper.readValue(inputDoc, ActuationDescriptor.class);
			return new Values(ad.getSoid(), ad.getId(), ad.getName(), this.mapper.writeValueAsString(ad.getAction()));
		} catch(Exception e){
			LOG.warn("Actuation malformed: " + inputDoc, e);

		}
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("soid", "id", "name", "action");
	}

}
