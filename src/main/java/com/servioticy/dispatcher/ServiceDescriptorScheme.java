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
package com.servioticy.dispatcher;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class ServiceDescriptorScheme implements Scheme {

	public List<Object> deserialize(byte[] bytes) {
		try {
			String input = new String(bytes, "UTF-8");
			String[] inputFields = input.split(";", 2);
			
			String[] ids = inputFields[0].split("-", 4);
			return new Values(inputFields[0].split("-", 2)[1], ids[0], ids[1], ids[2], inputFields[1]);
		} catch (UnsupportedEncodingException e) {
			// TODO Log the error
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		return new Fields("opid", "soid", "streamid", "su");
	}

}
