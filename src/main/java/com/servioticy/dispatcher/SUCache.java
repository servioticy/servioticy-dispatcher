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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class SUCache {
	private static final int CACHE_SIZE = 20;
	private Map<String, Long> lastTimestamps;
	private LinkedList<String> cached;
	private int cacheSize;
	
	public SUCache(){
		this(CACHE_SIZE);
	}
	public SUCache(int cacheSize){
		this.cacheSize = cacheSize;
		
		lastTimestamps = new HashMap<String, Long>();
		cached = new LinkedList<String>();
	}
	private void add(String subId, long timestamp){
		cached.add(subId);
		lastTimestamps.put(subId, timestamp);
		if(cached.size() > cacheSize){
			lastTimestamps.remove(cached.getFirst());
			cached.remove(0);
		}
	}
	private void remove(String subId){
		lastTimestamps.remove(subId);
		cached.remove(cached.indexOf(subId));
	}
	public boolean check(String subId, long timestamp){
		Long pastTS = lastTimestamps.get(subId);
		if(pastTS != null && timestamp <= pastTS){
			return true;
		}
		return false;
	}
	
	public void put(String subId, long timestamp){
		Long pastTS = lastTimestamps.get(subId);
		if(pastTS == null){
			this.add(subId, timestamp);
		}
		else if(timestamp > pastTS){
			this.remove(subId);
			this.add(subId, timestamp);
		}
		else{
			this.remove(subId);
			this.add(subId, pastTS);
		}
	}
}
