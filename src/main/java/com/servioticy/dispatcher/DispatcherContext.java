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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class DispatcherContext {
	static final public String DEFAULT_CONFIG_PATH = "/dispatcher.xml";
	
	static final public String PUBLISHER_LOCAL_ADDRESS = "publocaddress";
	static final public String PUBLISHER_EXTERNAL_ADDRESS = "pubextaddress";
	static final public String PUBLISHER_EXTERNAL_PORT = "pubextport";
	
	static final public String BOOTSTRAP_ADDRESS = "bstrapaddress";
	static final public String BOOTSTRAP_PORT = "bstrapport";

	public static String restBaseURL;
	public static String[] kestrelIPs;
	public static int kestrelPort;
	public static Map<String, Properties> pubProperties;
	public static Map<String, Properties> bootstrapsProperties;
	
	
	public static void loadConf(){
		String path = DispatcherContext.DEFAULT_CONFIG_PATH;
		HierarchicalConfiguration config;
		try {
			config = new XMLConfiguration(DispatcherContext.class.getResource(path));
			config.setExpressionEngine(new XPathExpressionEngine());
			
			if(config.containsKey("compoAPI")){
				DispatcherContext.restBaseURL = config.getString("compoAPI");
			}
			if(config.containsKey("kestrels/kestrel[1]/ip")){
				ArrayList<String> kestrel = new ArrayList<String>();
				for(int i=1; config.containsKey("kestrels/kestrel[" + i + "]/ip"); i++){
					kestrel.add(config.getString("kestrels/kestrel[" + i + "]/ip"));
				}
				DispatcherContext.kestrelIPs = (String[]) kestrel.toArray(new String[]{});
			}
			if(config.containsKey("kestrels/port")){
				DispatcherContext.kestrelPort = config.getInt("kestrels/port");
			}
			if(config.containsKey("pubsub/selfAddresses/selfAddress[1]/computerName")){
				Map<String, Properties> pubProperties = new HashMap<String, Properties>();
				for(int i=1; config.containsKey("pubsub/selfAddresses/selfAddress[" + i + "]/computerName"); i++){
					Properties props = new Properties();
					if(config.containsKey("pubsub/selfAddresses/selfAddress[" + i + "]/localAddress")){
						props.setProperty(DispatcherContext.PUBLISHER_LOCAL_ADDRESS, config.getString("pubsub/selfAddresses/selfAddress[" + i + "]/localAddress"));
					}
					if(config.containsKey("pubsub/selfAddresses/selfAddress[" + i + "]/externalAddress")){
						props.setProperty(DispatcherContext.PUBLISHER_EXTERNAL_ADDRESS, config.getString("pubsub/selfAddresses/selfAddress[" + i + "]/externalAddress"));
					}
					if(config.containsKey("pubsub/selfAddresses/selfAddress[" + i + "]/externalPort")){
						props.setProperty(DispatcherContext.PUBLISHER_EXTERNAL_PORT, config.getString("pubsub/selfAddresses/selfAddress[" + i + "]/externalPort"));
					}
					pubProperties.put(config.getString("pubsub/selfAddresses/selfAddress[" + i + "]/computerName"), props);
				}
				DispatcherContext.pubProperties = pubProperties;
			}
			if(config.containsKey("pubsub/bootstraps/bootstrap[1]/name")){
				Map<String, Properties> bootstrapsProperties = new HashMap<String, Properties>();
				for(int i=1; config.containsKey("pubsub/bootstraps/bootstrap[" + i + "]/name"); i++){
					Properties props = new Properties();
					if(config.containsKey("pubsub/bootstraps/bootstrap[" + i + "]/address")){
						props.setProperty(DispatcherContext.BOOTSTRAP_ADDRESS, config.getString("pubsub/bootstraps/bootstrap[" + i + "]/address"));
					}
					if(config.containsKey("pubsub/bootstraps/bootstrap[" + i + "]/port")){
						props.setProperty(DispatcherContext.BOOTSTRAP_PORT, config.getString("pubsub/bootstraps/bootstrap[" + i + "]/port"));
					}
					bootstrapsProperties.put(config.getString("pubsub/bootstraps/bootstrap[" + i + "]/name"), props);
				}
				DispatcherContext.bootstrapsProperties = bootstrapsProperties;
			}
			
		}catch(Exception e){
			e.printStackTrace();
			return;
		}
	}
}
