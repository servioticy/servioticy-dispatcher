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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class AliasReplacer {
	Map<String, String> aliases;
	public AliasReplacer(ArrayList<LinkedHashMap<String, String>> aliasesGroups){
		aliases = new HashMap<String, String>();
		
		if(aliasesGroups == null){
			return;
		}
		
		for(Map<String, String> aliasGroup: aliasesGroups){
			Map<String, String> doneAliasGroup = new HashMap<String, String>();
			for(Map.Entry<String, String> alias : aliasGroup.entrySet()){
				doneAliasGroup.put(alias.getKey(), replace(alias.getValue()));
			}
			aliases.putAll(doneAliasGroup);
		}
		
	}
	
	public String replace(String str){
		if(str == null){
			return str;
		}
		
		boolean[] transformedChars = new boolean[str.length()];
		SortedMap<Integer, String> assignations = new TreeMap<Integer, String>();
		
		// First we find find the aliases matches, without overlaps.
		// The replacement is done separately to avoid finding aliases in partly replaced substrings.
		for(Map.Entry<String, String> alias : aliases.entrySet()){
			for(int firstIndex = 0; firstIndex < str.length(); firstIndex++){
				firstIndex = str.indexOf(alias.getKey(), firstIndex);
				if(firstIndex == -1){
					break;
				}
				int lastIndex = firstIndex + alias.getKey().length()-1;
				boolean replaced = false;
				for(int i = firstIndex; i <= lastIndex; i++){
					if(transformedChars[i] == true){
						replaced = true;
						break;
					}
				}
				if(replaced){
					continue;
				}
				assignations.put(firstIndex, alias.getKey());
				
				for(int i = 0; i < alias.getKey().length(); i++){
					transformedChars[firstIndex + i] = true;
				}
				firstIndex = lastIndex;
			}
		}
		// Now we perform actual the alias replacement.
		int indexOffset = 0;
		for(Map.Entry<Integer, String> assignation : assignations.entrySet()){
			String alias = assignation.getValue();
			String replacement = aliases.get(alias);
			int firstIndex = assignation.getKey() + indexOffset;
			int lastIndex = firstIndex + alias.length()-1;
			
			String startStr = (firstIndex == 0) ? "" : str.substring(0, firstIndex);
			String endStr = (lastIndex == str.length()-1) ? "" : str.substring(lastIndex+1, str.length());
			// Replace the String part of the alias
			str = startStr + replacement + endStr;
			
			indexOffset += replacement.length() - alias.length();
		}
		return str;
		
	}
}
