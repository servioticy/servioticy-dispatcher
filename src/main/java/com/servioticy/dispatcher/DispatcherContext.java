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

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class DispatcherContext {
    static final public String DEFAULT_CONFIG_PATH = "dispatcher.xml";

    static final public String PUBLISHER_LOCAL_ADDRESS = "publocaddress";
    static final public String PUBLISHER_EXTERNAL_ADDRESS = "pubextaddress";
    static final public String PUBLISHER_EXTERNAL_PORT = "pubextport";

    static final public String BOOTSTRAP_ADDRESS = "bstrapaddress";
    static final public String BOOTSTRAP_PORT = "bstrapport";

    static final public String MQTT_ADDRESS = "mqttaddr";
    static final public String MQTT_PORT = "mqttport";

    public static String restBaseURL = "localhost";
    public static String[] kestrelAddresses = new String[]{"localhost"};
    public static int kestrelPort = 2229;
    public static String kestrelQueue = "services";
    public static Map<String, Properties> pubProperties;
    public static Map<String, Properties> bootstrapsProperties;
    public static Map<String, Properties> mqttProperties;


    public static void loadConf(String path) {

        HierarchicalConfiguration config;

        try {
            if (path == null) {
                config = new XMLConfiguration(DispatcherContext.DEFAULT_CONFIG_PATH);
            } else {
                config = new XMLConfiguration(path);
            }
            config.setExpressionEngine(new XPathExpressionEngine());

            DispatcherContext.restBaseURL = config.getString("servioticyAPI", DispatcherContext.restBaseURL);

            ArrayList<String> kestrel = new ArrayList<String>();
            if (config.containsKey("kestrels/kestrel[1]/addr")) {
                for (int i = 1; config.containsKey("kestrels/kestrel[" + i + "]/addr"); i++) {
                    kestrel.add(config.getString("kestrels/kestrel[" + i + "]/addr"));
                }
            } else {
                kestrel.add(DispatcherContext.kestrelAddresses[0]);
            }
            DispatcherContext.kestrelAddresses = (String[]) kestrel.toArray(new String[]{});

            DispatcherContext.kestrelPort = config.getInt("kestrels/port", DispatcherContext.kestrelPort);
            DispatcherContext.kestrelQueue = config.getString("kestrels/queue", DispatcherContext.kestrelQueue);
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

            if(config.containsKey("pubsub/mqtt/server[1]/name")){
                Map<String, Properties> mqttProperties = new HashMap<String, Properties>();
                for(int i=1; config.containsKey("pubsub/mqtt/server[" + i + "]/name"); i++){
                    Properties props = new Properties();
                    if(config.containsKey("pubsub/mqtt/server[" + i + "]/address")){
                        props.setProperty(DispatcherContext.MQTT_ADDRESS, config.getString("pubsub/mqtt/server[" + i + "]/address"));
                    }
                    if(config.containsKey("pubsub/mqtt/server[" + i + "]/port")){
                        props.setProperty(DispatcherContext.MQTT_PORT, config.getString("pubsub/mqtt/server[" + i + "]/port"));
                    }
                    mqttProperties.put(config.getString("pubsub/mqtt/server[" + i + "]/name"), props);
                }
                DispatcherContext.mqttProperties = mqttProperties;
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
