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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.UpdateDescriptor;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class UpdateDescriptorScheme implements Scheme {
	private ObjectMapper mapper;
	public UpdateDescriptorScheme(){
		this.mapper = new ObjectMapper();
	}
	public List<Object> deserialize(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			Object o = in.readObject();
			bis.close();
			String inputDoc = (String) o;
			UpdateDescriptor ud = this.mapper.readValue(inputDoc, UpdateDescriptor.class);
			return new Values(ud.getSoid(), ud.getStreamid(), this.mapper.writeValueAsString(ud.getSu()));
		} catch(Exception e){
			// TODO Log the error
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		return new Fields("soid", "streamid", "su");
	}

}
