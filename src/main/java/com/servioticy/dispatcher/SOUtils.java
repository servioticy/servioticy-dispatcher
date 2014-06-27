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

import com.servioticy.datamodel.*;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.*;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 */
public class SOUtils{
    public static final int TYPE_SIMPLE = 0;
    public static final int TYPE_NUMBER = 0;
    public static final int TYPE_BOOLEAN = 1;
    public static final int TYPE_STRING = 2;

    public static final int TYPE_ARRAY = 3;
    public static final int TYPE_ARRAY_NUMBER = 3;
    public static final int TYPE_ARRAY_BOOLEAN = 4;
    public static final int TYPE_ARRAY_STRING = 5;

    public SO so;

    public SOUtils(SO so){
        this.so = so;
    }

    public static int findClosing(String str, int beginIndex, int endIndex, String closing) {
        String openKey = str.substring(beginIndex, endIndex);
        int strIndex = endIndex;
        int counter = 1;
        int beginCloseIndex = -1;
        while (counter > 0) {
            int beginOpenIndex = str.indexOf(openKey, strIndex);
            beginCloseIndex = str.indexOf(closing, strIndex);
            if (beginOpenIndex < beginCloseIndex) {
                counter++;
                strIndex = beginOpenIndex + closing.length();
            } else if (beginCloseIndex < beginOpenIndex) {
                counter--;
                strIndex = beginCloseIndex + closing.length();
            } else {
                return -1;
            }
        }
        return beginCloseIndex;
    }

    public static boolean isFunction(String func) {
        if (func == null) {
            return false;
        }
        String trimmed = func.trim();
        int beginArgsIndex = trimmed.indexOf("(");
        int endArgsIndex = findClosing(trimmed, beginArgsIndex, beginArgsIndex + 1, ")");
        int beginFuncIndex = trimmed.indexOf("(");
        int endFuncIndex = findClosing(trimmed, beginFuncIndex, beginFuncIndex + 1, "}");

        if (trimmed.indexOf("function") < beginArgsIndex &&
                beginArgsIndex < trimmed.indexOf(')') &&
                endArgsIndex != -1 &&
                endArgsIndex < beginFuncIndex &&
                beginFuncIndex < trimmed.indexOf('}') &&
                endFuncIndex != -1 &&
                (trimmed.indexOf(';') == -1 || trimmed.indexOf(';') > beginFuncIndex) &&
                endFuncIndex == (trimmed.length() - 1)) {

            return true;
        }

        return false;
    }

    public static String functionArgsString(String func){
        int beginIndex = func.indexOf('(');
        int endIndex = findClosing(func, beginIndex, beginIndex + 1, ")");

        return func.substring(beginIndex + 1, endIndex).trim();
    }

    public static List<String> functionArgs(String func){
        String argsStr = functionArgsString(func);
        if(argsStr == null){
            return null;
        }
        ArrayList<String> args = new ArrayList<String>(Arrays.asList(argsStr.split(",")));
        for(String arg : args){
            arg = arg.trim();
        }
        return args;
    }

    public Set<String> getStreamsBySourceId(String sourceId){
        Set<String> streams = new HashSet<String>();
        for(Map.Entry<String, SOStream> streamEntry : this.so.getStreams().entrySet()){
            String streamId = streamEntry.getKey();
            SOStream stream = streamEntry.getValue();
            for(Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()){
                String channelId = channelEntry.getKey();
                SOChannel channel = channelEntry.getValue();

                String cvFunction = channel.getCurrentValue();

                if(cvFunction == null){
                    continue;
                }

                for(String arg : functionArgs(cvFunction)){
                    if(arg.equals(sourceId)){
                        streams.add(streamId);
                    }
                }

            }
        }
        return streams;
    }

    public Set<String> getSourceIdsByStream(String streamId){
        Set<String> sourceIds = new HashSet<String>();
        SOStream stream = so.getStreams().get(streamId);
        sourceIds.addAll(functionArgs(stream.getPreFilter()));
        sourceIds.addAll(functionArgs(stream.getPostFilter()));
        for(Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()){
            String channelId = channelEntry.getKey();
            SOChannel channel = channelEntry.getValue();

            sourceIds.addAll(functionArgs(channel.getCurrentValue()));
        }
        return sourceIds;
    }

    public String initializationCode(Map<String, String> jsons){
        String result = "";
        for(Map.Entry<String, String> jsonEntry : jsons.entrySet()){
            result += "var " + jsonEntry.getKey() + " = " + jsonEntry.getValue() + ";";
        }
        return result;
    }

    public boolean checkPreFilter(String streamId, Map<String, String> inputJsons) throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        SOStream stream = so.getStreams().get(streamId);
        if (stream.getPreFilter() == null || stream.getPreFilter().trim().equals("")) {
            return true;
        }
        String preFilterCode = stream.getPreFilter();
        String resultVar = "$" + Long.toHexString(UUID.randomUUID().getMostSignificantBits());

        engine.eval(initializationCode(inputJsons) + "var " + resultVar + "=Boolean(" + preFilterCode + "(" +  functionArgsString(preFilterCode) + "));");
        return (Boolean) engine.get(resultVar);

    }

    private int parseType(String type){
        type = type.toLowerCase().trim();

        int typeCode = TYPE_SIMPLE;

        if(type.startsWith("array")){
            String arrayType =  type.substring("array".length(), type.length());
            if(arrayType == null){
                return -1;
            }
            arrayType = arrayType.trim();
            if(!arrayType.startsWith("(") || !arrayType.endsWith(")")){
                return -1;
            }
            arrayType = arrayType.substring(1, arrayType.length()-1);
            if(arrayType == null){
                return -1;
            }
            arrayType = arrayType.trim();
            type = arrayType;
            typeCode = TYPE_ARRAY;
        }

        if (type.equals("number")) {
            return typeCode + TYPE_NUMBER;
        } else if (type.equals("boolean")) {
            return typeCode + TYPE_BOOLEAN;
        } else if (type.equals("string")) {
            return typeCode + TYPE_STRING;
        }

        return -1;
    }

    public SensorUpdate getResultSU(String streamId, Map<String, String> inputJsons, long timestamp) throws JsonParseException, JsonMappingException, IOException, ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");

        SensorUpdate su = new SensorUpdate();

        su.setLastUpdate(timestamp);
        su.setChannels(new LinkedHashMap<String, SUChannel>());

        SOStream stream = so.getStreams().get(streamId);
        for (Map.Entry<String, SOChannel> channelEntry : stream.getChannels().entrySet()) {
            SOChannel channel = channelEntry.getValue();
            SUChannel suChannel = new SUChannel();
            String currentValueCode = channel.getCurrentValue();
            // TODO Check for invalid calls
            // TODO Check for an invalid header
            if (isFunction(currentValueCode)) {
                // There is no code for one of the channels. Invalid.
                return null;
            } else {
                String type;
                boolean array = false;

                switch(parseType(channel.getType())) {
                    case TYPE_ARRAY_NUMBER:
                        array = true;
                    case TYPE_NUMBER:
                        type = "number";
                        break;
                    case TYPE_ARRAY_BOOLEAN:
                        array = true;
                    case TYPE_BOOLEAN:
                        type = "boolean";
                        break;
                    case TYPE_ARRAY_STRING:
                        array = true;
                    case TYPE_STRING:
                        type = "string";
                        break;
                    default:
                        return null;
                }

                String resultVar = "$" + Long.toHexString(UUID.randomUUID().getMostSignificantBits());
                String finalCode;
                if(!array) {
                    finalCode = initializationCode(inputJsons) +
                            "var " + resultVar + " = " + currentValueCode + "(" + functionArgsString(currentValueCode) + ");" +
                            "if(typeof " + resultVar + " !== '" + type + "'){" +
                            resultVar + " = null;" +
                            "}";

                }else {
                    finalCode = initializationCode(inputJsons) +
                            "var " + resultVar + " = " + currentValueCode + "(" + functionArgsString(currentValueCode) + ");" +
                            "if(Object.prototype.toString.call(" + resultVar + ") !== '[object Array]') {" +
                            resultVar + " = null;" +
                            "}" +
                            "else {" +
                            "for(var i = 0; i < " + resultVar + ".length; i++){" +
                            "if(typeof " + resultVar + "[i] !== '" + type + "'){" +
                            resultVar + " = null;" +
                            "break;" +
                            "}" +
                            "}" +
                            "}";
                }

                engine.eval(finalCode);
                if (engine.get(resultVar) == null) {
                    // Filtered output. The type is not the expected one.
                    return null;
                }
                suChannel.setCurrentValue(engine.get(resultVar));
            }
            suChannel.setUnit(channel.getUnit());

            su.getChannels().put(channelEntry.getKey(), suChannel);
        }

        su.setStreamsChain(new ArrayList<ArrayList<String>>());

        su.setTimestampChain(new ArrayList<Long>());

        return su;
    }

    public boolean checkPostFilter(String streamId, Map<String, String> inputJsons) throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        SOStream stream = so.getStreams().get(streamId);
        if (stream.getPostFilter() == null || stream.getPostFilter().trim().equals("")) {
            return true;
        }
        String postFilterCode = stream.getPostFilter();
        String resultVar = "$" + Long.toHexString(UUID.randomUUID().getMostSignificantBits());

        String finalCode = initializationCode(inputJsons) + "var " + resultVar + "=Boolean(" + postFilterCode + "(" +  functionArgsString(postFilterCode) + "));";
        engine.eval(finalCode);
        return (Boolean) engine.get(resultVar);
    }

}
