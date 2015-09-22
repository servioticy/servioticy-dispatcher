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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.serviceobject.SO010;
import com.servioticy.datamodel.serviceobject.SOChannel;
import com.servioticy.datamodel.serviceobject.SOStream;
import com.servioticy.datamodel.serviceobject.SOStream010;
import com.servioticy.datamodel.sensorupdate.SUChannel;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.dispatcher.jsonprocessors.AliasReplacer;
import com.servioticy.dispatcher.jsonprocessors.JsonPathReplacer;
import org.elasticsearch.common.geo.GeoPoint;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class SOProcessor010 extends SOProcessor{

    private ObjectMapper mapper;
    AliasReplacer aliases;
    LinkedHashMap<String, PSOStream> streams;
    LinkedHashMap<String, Object> queries;
    SO010 so;
    Map<String, HashSet<String>> streamsByDocId;
    Map<String, HashSet<String>> docIdsByStream;

    public SOProcessor010(SO010 so, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException {

        this.mapper = mapper;
        this.streamsByDocId = new HashMap<String, HashSet<String>>();
        this.docIdsByStream = new HashMap<String, HashSet<String>>();

        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.so = so;
        aliases = new AliasReplacer(this.so.getAliases());

        // Streams
        this.streams = new LinkedHashMap<String, PSOStream>();

    }

    private void addStreamByDocIds(String stream, Set<String> docIds) {
        for (String docId : docIds) {
            if (!this.streamsByDocId.containsKey(docId)) {
                this.streamsByDocId.put(docId, new HashSet<String>());
            }
            this.streamsByDocId.get(docId).add(stream);
        }
    }

    public String replaceAliases() throws JsonGenerationException, JsonMappingException, IOException {
        for (Map.Entry<String, SOStream> streamEntry : this.so.getStreams(this.mapper).entrySet()) {
            SOStream010 stream = (SOStream010) streamEntry.getValue();
            for (Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()) {
                SOChannel channel = channelEntry.getValue();

                channel.setCurrentValue(aliases.replace(channel.getCurrentValue()));
                channel.setUnit(aliases.replace(channel.getUnit() == null ? "" : channel.getUnit()));
                channel.setType(aliases.replace(channel.getType()));
            }

            stream.setPreFilter(stream.getPreFilter() == null ? "true" : (aliases.replace(stream.getPreFilter())));
            stream.setPostFilter(stream.getPostFilter() == null ? "true" : aliases.replace(stream.getPostFilter()));
        }
        return this.mapper.writeValueAsString(this.so);
    }

    public void compileJSONPaths() {
        for (Map.Entry<String, SOStream> streamEntry : this.so.getStreams(this.mapper).entrySet()) {
            PSOStream pstream = new PSOStream();
            SOStream010 stream = (SOStream010) streamEntry.getValue();
            String streamId = streamEntry.getKey();

            this.docIdsByStream.put(streamId, new HashSet<String>());

            // Channels
            pstream.channels = new LinkedHashMap<String, PSOChannel>();
            for (Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()) {
                PSOChannel pchannel = new PSOChannel();
                SOChannel channel = channelEntry.getValue();
                pchannel.currentValue = null;
                if (channel.getCurrentValue() != null) {
                    pchannel.currentValue = new JsonPathReplacer(channel.getCurrentValue(), mapper);
                    // Set the objective streams for each group of SUs
                    Set<String> docIds = pchannel.currentValue.getJsonPathIds();
                    addStreamByDocIds(streamEntry.getKey(), docIds);
                    this.docIdsByStream.get(streamId).addAll(docIds);
                }

                pchannel.type = channel.getType();

                pstream.channels.put(channelEntry.getKey(), pchannel);
            }
            pstream.preFilter = null;
            if (stream.getPreFilter() != null) {
                pstream.preFilter = new JsonPathReplacer(stream.getPreFilter(), mapper);
                this.docIdsByStream.get(streamId).addAll(pstream.preFilter.getJsonPathIds());
                addStreamByDocIds(streamId, pstream.preFilter.getJsonPathIds());
            }


            pstream.postFilter = null;
            if (stream.getPostFilter() != null) {
                pstream.postFilter = new JsonPathReplacer(stream.getPostFilter(), mapper);
                this.docIdsByStream.get(streamId).addAll(pstream.postFilter.getJsonPathIds());
                addStreamByDocIds(streamId, pstream.postFilter.getJsonPathIds());
            }

            this.streams.put(streamEntry.getKey(), pstream);
        }
    }

    public Set<String> getStreamsBySourceId(String docId) {
        Set<String> result = new HashSet<String>();
        if (this.streamsByDocId.get(docId) != null) {
            result.addAll(this.streamsByDocId.get(docId));
        }
        return result;
    }

    public Set<String> getSourceIdsByStream(String streamid) {
        Set<String> result = new HashSet<String>();
        if (this.docIdsByStream.get(streamid) != null) {
            result.addAll(this.docIdsByStream.get(streamid));
        }
        return result;
    }

    public boolean checkFilter(JsonPathReplacer filterField, Map<String, String> inputJsons) throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        if (filterField == null) {
            return true;
        }
        String filterCode = filterField.replace(inputJsons);

        engine.eval("var result = Boolean(" + filterCode + ")");
        return (Boolean) engine.get("result");

    }

    public SensorUpdate getResultSU(String streamId, Map<String, SensorUpdate> inputSUs, String origin, long timestamp) throws JsonParseException, JsonMappingException, IOException, ScriptException {

        Map<String, String> inputDocs = new HashMap<String, String>();
        for(Map.Entry<String,SensorUpdate> inputSUEntry: inputSUs.entrySet()){
            inputDocs.put(inputSUEntry.getKey(), this.mapper.writeValueAsString(inputSUEntry.getValue()));
        }
        PSOStream pstream = this.streams.get(streamId);
        if (!checkFilter(pstream.preFilter, inputDocs)){
            return null;
        }
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");

        SensorUpdate su = new SensorUpdate();

        su.setLastUpdate(timestamp);
        su.setChannels(new LinkedHashMap<String, SUChannel>());

        int nulls = 0;
        for (Entry<String, PSOChannel> channelEntry : pstream.channels.entrySet()) {
            PSOChannel pchannel = channelEntry.getValue();
            SUChannel suChannel = new SUChannel();
            if (pchannel.currentValue == null) {
                suChannel.setCurrentValue(null);
                nulls++;
            } else {
                String currentValueCode = pchannel.currentValue.replace(inputDocs);
                Class type;
                String typeName;
                Object result = null;
                typeName = pchannel.type.toLowerCase();
                if (typeName.equals("number")) {
                    type = Double.class;
                } else if (typeName.equals("boolean")) {
                    type = Boolean.class;
                } else if (typeName.equals("string")) {
                    type = String.class;
                }
                else if (typeName.equals("geo_point")){
                    type = GeoPoint.class;

                }
                else {
                    return null;
                }

                engine.eval("var result = JSON.stringify(" + currentValueCode + ")");
                result = this.mapper.readValue((String)engine.get("result"), type);
                if(type == GeoPoint.class)
                    result = ((GeoPoint)result).getLat()+","+((GeoPoint)result).getLon();
                suChannel.setCurrentValue(result);

            }
            suChannel.setUnit(pchannel.unit);

            su.getChannels().put(channelEntry.getKey(), suChannel);
        }

        if (nulls >= su.getChannels().size()) {
            // This stream is mapping a Web Object.
            return null;
        }

        su.setTriggerPath(new ArrayList<ArrayList<String>>());

        su.setPathTimestamps(new ArrayList<Long>());

        this.mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String resultSUDoc = this.mapper.writeValueAsString(su);
        if(!inputDocs.containsKey("result")){
            inputDocs.put("result", resultSUDoc);
        }

        if (!checkFilter(pstream.postFilter, inputDocs)){
            return null;
        }
        return su;
    }

    private class PSOStream {
        public LinkedHashMap<String, PSOChannel> channels;
        public JsonPathReplacer preFilter;
        public JsonPathReplacer postFilter;
    }

    private class PSOChannel {
        public JsonPathReplacer currentValue;
        public String unit;
        public String type;
    }
}
