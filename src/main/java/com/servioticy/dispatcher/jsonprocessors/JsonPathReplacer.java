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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class JsonPathReplacer {
    ObjectMapper mapper;
    private String str;
    // JsonPaths with their group id pointing to a position of the str. Several JsonPaths in a row at the same position
    // 	will be inside the list.
    private SortedMap<Integer, LinkedList<Map.Entry<String, JsonPath>>> jsonPaths;

    public JsonPathReplacer(String str, ObjectMapper mapper) {
        this(str, mapper, new HashSet<String>());
        this.mapper = mapper;
    }

    public JsonPathReplacer(String str, ObjectMapper mapper, Set<String> excludedDocIds) {
        this.mapper = mapper;
        this.jsonPaths = new TreeMap<Integer, LinkedList<Map.Entry<String, JsonPath>>>();
        this.str = str;

        // compile JsonPaths on the String
        for (int firstIndex = 0; firstIndex < this.str.length(); ) {
            firstIndex = this.str.indexOf("{", firstIndex);

            if (firstIndex == -1) {
                break;
            }
            if ((firstIndex != 0) && (this.str.charAt(firstIndex - 1) == '\\')) {
                firstIndex++;
                continue;
            }

            int lastIndex;
            for (lastIndex = firstIndex; lastIndex < this.str.length(); lastIndex++) {
                lastIndex = this.str.indexOf("}", lastIndex);
                if (lastIndex == -1) {
                    break;
                }
                // Ignore if escaped.
                if (this.str.charAt(lastIndex - 1) == '\\') {
                    lastIndex++;
                    continue;
                }
                break;
            }
            if (lastIndex == -1) {
                break;
            }

            String jsonPath = this.str.substring(firstIndex + 1, lastIndex);
            String jsonId = "";
            // Check that right after the '{' char there is the '$' character (may be spaces in between). Then get
            //	the substring between the '$' and the '.' and use it as JSON id.
            int iDollar = jsonPath.indexOf("$");
            int endJsonId = jsonPath.indexOf('.', iDollar);
            endJsonId = endJsonId != -1 ? endJsonId : jsonPath.length();
            if (iDollar == -1) {
                firstIndex = lastIndex;
                continue;
            }

//			// Exclude jsonIds
//			
//			if(excludedDocIds.contains(jsonId)){
//				firstIndex++;
//				continue;
//			}

            // Change the index of the JsonPaths already existing in positions further than firstIndex.
            // Also, if any of those JsonPaths are before lastIndex, ignore it.
//			int indexOffset = (lastIndex-1) - (firstIndex+1);
//			boolean recJsonPath = false;

            // If there is a jsonId, extract it from the jsonPath and make the jsonPath 'standard'
            if (iDollar + 1 != endJsonId) {
                jsonId = jsonPath.substring(iDollar + 1, endJsonId);
                jsonPath = deleteSubstring(jsonPath, iDollar + 1, endJsonId);
            }

            // Escape \\{ and \\}
            unescapeChar(jsonPath, '{');
            unescapeChar(jsonPath, '}');
            JsonPath jp;
            try {
                jp = JsonPath.compile(jsonPath);
            } catch (java.lang.IllegalArgumentException e) {
                // If the detected JsonPath is invalid for any reason, simply ignore it.
                firstIndex++;
                continue;
            }

//			// jpstr.jsonPaths is ordered by key.
//			for(Entry<Integer, LinkedList<Entry<String, JsonPath>>> currentJpList : this.jsonPaths.entrySet()){
//				if(currentJpList.getKey() > firstIndex){
//					if(currentJpList.getKey() <= lastIndex){
//						recJsonPath = true;
//						break;
//					}
//					int newIndex = currentJpList.getKey() - indexOffset;
//					if(this.jsonPaths.get(newIndex) != null){
//						this.jsonPaths.get(newIndex).addAll(currentJpList.getValue());
//					}
//					else{
//						this.jsonPaths.put(newIndex, currentJpList.getValue());
//					}
//					this.jsonPaths.remove(currentJpList.getKey());
//				}
//			}
//			if(recJsonPath){
//				firstIndex++;
//				continue;
//			}
            LinkedList<Entry<String, JsonPath>> jplist = new LinkedList<Entry<String, JsonPath>>();
            jplist.add(new AbstractMap.SimpleEntry<String, JsonPath>(jsonId, jp));

            if (this.jsonPaths.get(firstIndex) != null) {
                this.jsonPaths.get(firstIndex).addAll(jplist);
            } else {
                this.jsonPaths.put(firstIndex, jplist);
            }

            this.str = deleteSubstring(this.str, firstIndex, lastIndex + 1);
        }
        // Escape \\{ and \\}
        unescapeChar(this.str, '{');
        unescapeChar(this.str, '}');
    }

    public Set<String> getJsonPathIds() {
        Set<String> jpids = new HashSet<String>();

        for (Entry<Integer, LinkedList<Entry<String, JsonPath>>> jpPosEntry : this.jsonPaths.entrySet()) {
            for (Entry<String, JsonPath> jp : jpPosEntry.getValue()) {
                jpids.add(jp.getKey());
            }
        }

        return jpids;
    }

    public String replace(Map<String, String> jsons) throws InvalidPathException {
        if (this.str == null) {
            this.str = "";
        }
        String result = this.str;
        int indexOffset = 0;
        StringBuilder sb = new StringBuilder(result);
        for (Map.Entry<Integer, LinkedList<Map.Entry<String, JsonPath>>> jpsReplacement : this.jsonPaths.entrySet()) {
            int index = jpsReplacement.getKey() + indexOffset;
            LinkedList<Map.Entry<String, JsonPath>> jps = jpsReplacement.getValue();

//			String startStr = (index == 0) ? "" : result.substring(0, index);
//			String endStr = (index == result.length()) ? "" : result.substring(index, result.length());

            String partial = "";

            for (Map.Entry<String, JsonPath> jp : jps) {
//				if(jp.isPathDefinite()){
                String json;
                String key = jp.getKey();
                try {
                    // If the path does not exist in the input json, throws InvalidPathException
                    json = jsons.get(key);
                    Object content = jp.getValue().read(json);
//                    if (content instanceof String) {
//                        partial += "'" + content + "'";
//                    } else{
                        partial += this.mapper.writeValueAsString(content);
//                    }
                } catch (java.lang.IllegalArgumentException e) {
                    // The input is not a json
                    // TODO This should be done *only* on queries. In navigations of implicit queries or selfdocument, it should be an error
                    //	and the document shouldn't be processed.
                    e.printStackTrace();
                    partial += "null";
                } catch (Exception e) {
                    // Not a correct input json
                    // TODO log this
                    e.printStackTrace();
                    partial += "null";
                }
//				}
            }
            if (index > sb.toString().length()) {
                sb.append(partial);
            } else {
                sb.replace(index, index, partial);
            }

//			result = startStr + partial + endStr;
            indexOffset += partial.length();

        }
        result = sb.toString();

        return result;
    }

    private String deleteSubstring(String str, int firstIndex, int lastIndex) {
        String startStr = (firstIndex == 0) ? "" : str.substring(0, firstIndex);
        String endStr = (lastIndex == str.length()) ? "" : str.substring(lastIndex, str.length());
        return startStr + endStr;
    }

    private String unescapeChar(String str, char escaped) {
        for (int i = 0; i < str.length() - 1; ) {
            i = str.indexOf("\\", i);

            if (i == -1) {
                break;
            }
            if (str.charAt(i + 1) == escaped) {
                StringBuilder sb = new StringBuilder(str);
                sb.deleteCharAt(i);
                str = sb.toString();
                continue;
            }
            i++;
        }
        return str;
    }
}
