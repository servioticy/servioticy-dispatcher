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

import java.util.HashMap;
import java.util.Map;

import com.servioticy.datamodel.ExternalSubscription;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class PubSubProcessor {
	private JsonPathReplacer url;
	private JsonPathReplacer body;
	
	private AliasReplacer aliases;
	
	private ExternalSubscription subscription;
	
	public PubSubProcessor(ExternalSubscription subscription){
		this.subscription = subscription;
		this.aliases = new AliasReplacer(subscription.getAliases());
		
		this.url = new JsonPathReplacer(aliases.replace(subscription.getDestination()));
		this.body = new JsonPathReplacer(aliases.replace(subscription.getBody()));
	}
	
	public ExternalSubscription replaceAliases(){	
		
		this.subscription.setDestination( aliases.replace(subscription.getDestination()) );
		this.subscription.setBody( aliases.replace(subscription.getBody()) );
		
		return this.subscription;
	}
	
	public void compileJSONPaths(){
		this.url = new JsonPathReplacer(this.subscription.getDestination());
		this.body = new JsonPathReplacer(this.subscription.getBody());
	}
	
	public String getUrl(String inputJson){
		Map<String, String> inputJsons = new HashMap<String, String>();
		inputJsons.put("", inputJson);
		
		return this.url.replace(inputJsons);
	}
	
	public String getBody(String inputJson){
		Map<String, String> inputJsons = new HashMap<String, String>();
		inputJsons.put("", inputJson);
		return this.body.replace(inputJsons);
	}
}
