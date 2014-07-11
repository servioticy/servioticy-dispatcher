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

import com.servioticy.datamodel.*;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.mozilla.javascript.Provelement;
import org.mozilla.javascript.Provenance;
import org.mozilla.javascript.ProvenanceAPI;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class SOProcessor {

    String id;
    AliasReplacer aliases;
    LinkedHashMap<String, PSOStream> streams;
    LinkedHashMap<String, Object> queries;
    SO so;
    String jso;
    Map<String, HashSet<String>> streamsByDocId;
    Map<String, HashSet<String>> docIdsByStream;

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

    private void addStreamByDocIds(String stream, Set<String> docIds) {
        for (String docId : docIds) {
            if (!this.streamsByDocId.containsKey(docId)) {
                this.streamsByDocId.put(docId, new HashSet<String>());
            }
            this.streamsByDocId.get(docId).add(stream);
        }
    }

    public String replaceAliases() throws JsonGenerationException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        for (Map.Entry<String, SOStream> streamEntry : this.so.getStreams().entrySet()) {
            SOStream stream = streamEntry.getValue();
            for (Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()) {
                SOChannel channel = channelEntry.getValue();

                channel.setCurrentValue(aliases.replace(channel.getCurrentValue()));
                channel.setUnit(aliases.replace(channel.getUnit() == null ? "" : channel.getUnit()));
                channel.setType(aliases.replace(channel.getType()));
            }

            stream.setPreFilter(stream.getPreFilter() == null ? "true" : (aliases.replace(stream.getPreFilter())));
            stream.setPostFilter(stream.getPostFilter() == null ? "true" : aliases.replace(stream.getPostFilter()));
        }
        return mapper.writeValueAsString(this.so);
    }

    public void compileJSONPaths() {
        for (Map.Entry<String, SOStream> streamEntry : this.so.getStreams().entrySet()) {
            PSOStream pstream = new PSOStream();
            SOStream stream = streamEntry.getValue();
            String streamId = streamEntry.getKey();

            this.docIdsByStream.put(streamId, new HashSet<String>());

            // Channels
            pstream.channels = new LinkedHashMap<String, PSOChannel>();
            for (Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()) {
                PSOChannel pchannel = new PSOChannel();
                SOChannel channel = channelEntry.getValue();
                pchannel.currentValue = null;
                if (channel.getCurrentValue() != null) {
                    pchannel.currentValue = new JsonPathReplacer(channel.getCurrentValue());
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
                pstream.preFilter = new JsonPathReplacer(stream.getPreFilter());
                this.docIdsByStream.get(streamId).addAll(pstream.preFilter.getJsonPathIds());
                addStreamByDocIds(streamId, pstream.preFilter.getJsonPathIds());
            }


            pstream.postFilter = null;
            if (stream.getPreFilter() != null) {
                pstream.postFilter = new JsonPathReplacer(stream.getPostFilter());
                this.docIdsByStream.get(streamId).addAll(pstream.postFilter.getJsonPathIds());
                addStreamByDocIds(streamId, pstream.postFilter.getJsonPathIds());
            }

            this.streams.put(streamEntry.getKey(), pstream);
        }
    }

    public Set<String> getStreamsByDocId(String docId) {
        Set<String> result = new HashSet<String>();
        if (this.streamsByDocId.get(docId) != null) {
            result.addAll(this.streamsByDocId.get(docId));
        }
        return result;
    }

    public Set<String> getDocIdsByStream(String streamid) {
        Set<String> result = new HashSet<String>();
        if (this.docIdsByStream.get(streamid) != null) {
            result.addAll(this.docIdsByStream.get(streamid));
        }
        return result;
    }

    public boolean checkPreFilter(String streamId, Map<String, String> inputJsons, List<Provelement> provList, Map<String, String> mapVarSU) throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        PSOStream pstream = this.streams.get(streamId);
        if (pstream.preFilter == null) {
            return true;
        }
        HashMap<String, String> inputVar = new HashMap();
        String preFilterCode = pstream.preFilter.replace(inputJsons, inputVar, mapVarSU);

        inputVar.put(ProvenanceAPI.COMPUTATION, "Boolean(" + preFilterCode + ")");
        String fullComputationString = ProvenanceAPI.buildString(inputVar);

        List<Provelement> newProvList = (List<Provelement>)ProvenanceAPI.executeWithProv(fullComputationString, provList);

        provList.clear();
        provList.addAll(newProvList);

        String result = (String) ProvenanceAPI.getResultValue(provList);


        return Boolean.parseBoolean(result);
        //engine.eval("var result = Boolean(" + preFilterCode + ")");
        //return (Boolean) engine.get("result");

    }

    public SensorUpdate getResultSU(String streamId, Map<String, String> inputJsons, long timestamp, List<Provelement> provList, Map<String, String> mapVarSU) throws JsonParseException, JsonMappingException, IOException, ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");

        ObjectMapper mapper = new ObjectMapper();

        SensorUpdate su = new SensorUpdate();

        su.setLastUpdate(timestamp);
        su.setChannels(new LinkedHashMap<String, SUChannel>());

        PSOStream pstream = this.streams.get(streamId);
        int nulls = 0;
        for (Entry<String, PSOChannel> channelEntry : pstream.channels.entrySet()) {
            PSOChannel pchannel = channelEntry.getValue();
            SUChannel suChannel = new SUChannel();
            if (pchannel.currentValue == null) {
                suChannel.setCurrentValue(null);
                nulls++;
            } else {
                HashMap<String, String> inputVar = new HashMap();
                String currentValueCode = pchannel.currentValue.replace(inputJsons, inputVar, mapVarSU);

                String type;

                if (pchannel.type.toLowerCase().equals("number")) {
                    type = "Number";
                } else if (pchannel.type.toLowerCase().equals("boolean")) {
                    type = "Boolean";
                } else if (pchannel.type.toLowerCase().equals("string")) {
                    type = "String";
                }
                // TODO Array type
                else {
                    return null;
                }


                inputVar.put(ProvenanceAPI.COMPUTATION, type + "(" + currentValueCode + ")");
                //engine.eval("var result = " + type + "(" + currentValueCode + ")");
                String fullComputationString = ProvenanceAPI.buildString(inputVar);

                List<Provelement> newProvList = (List<Provelement>)ProvenanceAPI.executeWithProv(fullComputationString, provList);

                provList.clear();
                provList.addAll(newProvList);

                Object result = null;
                if (pchannel.type.toLowerCase().equals("number")) {
                    result = Double.parseDouble((String) ProvenanceAPI.getResultValue(provList));
                } else if (pchannel.type.toLowerCase().equals("boolean")) {
                    result = Boolean.parseBoolean((String) ProvenanceAPI.getResultValue(provList));
                } else if (pchannel.type.toLowerCase().equals("string")) {
                    result = (String) ProvenanceAPI.getResultValue(provList);
                }
                // TODO Array type
                else {
                    return null;
                }

                suChannel.setCurrentValue(result);

                //suChannel.setCurrentValue(engine.get("result"));
            }
            suChannel.setUnit(pchannel.unit);

            su.getChannels().put(channelEntry.getKey(), suChannel);
        }

        if (nulls >= su.getChannels().size()) {
            // This stream is mapping a Web Object.
            return null;
        }

        su.setStreamsChain(new ArrayList<ArrayList<String>>());

        su.setTimestampChain(new ArrayList<Long>());

        return su;
    }

    public boolean checkPostFilter(String streamId, Map<String, String> inputJsons, List<Provelement> provList, Map<String, String> mapVarSU) throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        PSOStream pstream = this.streams.get(streamId);
        if (pstream.postFilter == null) {
            return true;
        }


        HashMap<String, String> inputVar = new HashMap();
        String postFilterCode = pstream.postFilter.replace(inputJsons, inputVar, mapVarSU);

        inputVar.put(ProvenanceAPI.COMPUTATION, "Boolean(" + postFilterCode + ")");
        String fullComputationString = ProvenanceAPI.buildString(inputVar);

        List<Provelement> newProvList = (List<Provelement>)ProvenanceAPI.executeWithProv(fullComputationString, provList);

        provList.clear();
        provList.addAll(newProvList);

        String result = (String) ProvenanceAPI.getResultValue(provList);

        return Boolean.parseBoolean(result);
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