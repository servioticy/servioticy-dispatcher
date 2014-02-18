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
package com.servioticy.dispatcher.jsonprocessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

import com.servioticy.datamodel.SO;
import com.servioticy.datamodel.SOChannel;
import com.servioticy.datamodel.SOStream;
import com.servioticy.datamodel.SUChannel;
import com.servioticy.datamodel.SensorUpdate;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class SOProcessor {
		
	private class PSOStream{
		public LinkedHashMap<String, PSOChannel> channels;
		public JsonPathReplacer preFilter;
		public JsonPathReplacer postFilter;
	}
	
	private class PSOChannel{
		public JsonPathReplacer currentValue;
		public String unit;
		public String type;
	}
	
	String id;
	
	AliasReplacer aliases;
	
	LinkedHashMap<String, PSOStream> streams;
	LinkedHashMap<String, Object> queries;
	
	SO so;
	String jso;
	
	Map<String, HashSet<String>> streamsByDocId;
	Map<String, HashSet<String>> docIdsByStream;
	
	private void addStreamByDocIds(String stream, Set<String> docIds){
		for(String docId: docIds){
			if(!this.streamsByDocId.containsKey(docId)){
				this.streamsByDocId.put(docId, new HashSet<String>());
			}
			this.streamsByDocId.get(docId).add(stream);
		}
	}
	
	public SOProcessor(String json, String soid) throws JsonParseException, JsonMappingException, IOException {
		this.id = soid;
				
		this.streamsByDocId = new HashMap<String, HashSet<String>>();
		this.docIdsByStream = new HashMap<String, HashSet<String>>();
		
		jso = json;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		this.so = mapper.readValue(json, SO.class);
		aliases = new AliasReplacer(this.so.getAliases());
		
		// Streams
		this.streams = new LinkedHashMap<String, PSOStream>();
		
	}
	
	public String replaceAliases() throws JsonGenerationException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		for(Map.Entry<String, SOStream> streamEntry: this.so.getStreams().entrySet()){
			SOStream stream = streamEntry.getValue();
			for(Map.Entry<String, SOChannel> channelEntry: stream.getChannels().entrySet()){
				SOChannel channel = channelEntry.getValue();
				
				channel.setCurrentValue( aliases.replace( channel.getCurrentValue() ) );
				channel.setUnit( aliases.replace( channel.getUnit() == null ? "" : channel.getUnit() ));
				channel.setType( aliases.replace( channel.getType() ) );
			}
			
			stream.setPreFilter( stream.getPreFilter() == null ? "true" : (aliases.replace( stream.getPreFilter()) ) );
			stream.setPostFilter( stream.getPostFilter() == null ? "true" : aliases.replace( stream.getPostFilter() ) );
		}
		return mapper.writeValueAsString(this.so);
	}
	
	public void compileJSONPaths(){
		for(Map.Entry<String, SOStream> streamEntry: this.so.getStreams().entrySet()){
			PSOStream pstream = new PSOStream();
			SOStream stream = streamEntry.getValue();
			String streamId = streamEntry.getKey();
			
			this.docIdsByStream.put(streamId, new HashSet<String>());
			
			// Channels
			pstream.channels = new LinkedHashMap<String, PSOChannel>();
			for(Map.Entry<String, SOChannel> channelEntry: stream.getChannels().entrySet()){
				PSOChannel pchannel = new PSOChannel();
				SOChannel channel = channelEntry.getValue();
				pchannel.currentValue =	new JsonPathReplacer(
												channel.getCurrentValue() 
											);
				
				// Set the objective streams for each group of SUs
				Set<String> docIds = pchannel.currentValue.getJsonPathIds();
				addStreamByDocIds(streamEntry.getKey(), docIds);
				this.docIdsByStream.get(streamId).addAll(docIds);
				
				pstream.channels.put(channelEntry.getKey(), pchannel);
			}
			
			pstream.preFilter =	new JsonPathReplacer(
										stream.getPreFilter()
									);
			this.docIdsByStream.get(streamId).addAll(pstream.preFilter.getJsonPathIds());
			addStreamByDocIds(streamId, pstream.preFilter.getJsonPathIds());
			
			Set<String> excludeDocIds = new HashSet<String>();
			pstream.postFilter = new JsonPathReplacer(
										stream.getPostFilter(),
										excludeDocIds
									);
			this.docIdsByStream.get(streamId).addAll(pstream.postFilter.getJsonPathIds());
			addStreamByDocIds(streamId, pstream.postFilter.getJsonPathIds());
			
			this.streams.put(streamEntry.getKey(), pstream);
		}
	}
	
	public Set<String> getStreamsByDocId(String docId){
		Set<String> result = new HashSet<String>();
		if(this.streamsByDocId.get(docId) != null){
			result.addAll(this.streamsByDocId.get(docId));
		}
		return result;
	}
	
	public Set<String> getDocIdsByStream(String streamid){
		Set<String> result = new HashSet<String>();
		if(this.docIdsByStream.get(streamid) != null){
			result.addAll(this.docIdsByStream.get(streamid));
		}
		return result;
	}
	
	public boolean checkPreFilter(String streamId, Map<String, String> inputJsons) throws ScriptException{
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine engine = factory.getEngineByName("JavaScript");
		PSOStream pstream = this.streams.get(streamId);
		String preFilterCode = pstream.preFilter.replace(inputJsons);
		
		engine.eval("var result = Boolean(" + preFilterCode + ")");
		return (Boolean) engine.get("result");

	}
	
	public SensorUpdate getResultSU(String streamId, Map<String, String> inputJsons, long timestamp) throws JsonParseException, JsonMappingException, IOException, ScriptException{		
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine engine = factory.getEngineByName("JavaScript");
		
		SensorUpdate su = new SensorUpdate();
		
		su.setLastUpdate(timestamp);
		su.setChannels(new LinkedHashMap<String, SUChannel>());
		
		PSOStream pstream = this.streams.get(streamId);
		for(Entry<String, PSOChannel> channelEntry: pstream.channels.entrySet()){
			PSOChannel pchannel = channelEntry.getValue();
			SUChannel suChannel = new SUChannel();
			String currentValueCode = pchannel.currentValue.replace(inputJsons);
			String type;
			
			if(pchannel.type.toLowerCase().equals("number")){
				type = "Number";
			}
			else if(pchannel.type.toLowerCase().equals("boolean")){
				type = "Boolean";
			}
			else if(pchannel.type.toLowerCase().equals("string")){
				type = "String";
			}
			// TODO Array type
			else{
				return null;
			}
			engine.eval("var result = " + type + "(" + currentValueCode + ")");
			
			suChannel.setCurrentValue(engine.get("result"));
			suChannel.setUnit(pchannel.unit);
			
			su.getChannels().put(channelEntry.getKey(), suChannel);
		}
		
		return su;
	}
	
	public boolean checkPostFilter(String streamId, Map<String, String> inputJsons) throws ScriptException{
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine engine = factory.getEngineByName("JavaScript");
		PSOStream pstream = this.streams.get(streamId);
		String postFilterCode = pstream.postFilter.replace(inputJsons);
		
		engine.eval("var result = Boolean(" + postFilterCode + ")");
		return (Boolean) engine.get("result");
	}
}
