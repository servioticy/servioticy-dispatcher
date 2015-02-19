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

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class DispatcherContext implements Serializable{
    static final public String DEFAULT_CONFIG_PATH = "dispatcher.xml";
    private static final long serialVersionUID = 1L;
    public String restBaseURL = "localhost";
    public String[] updatesAddresses = new String[]{"localhost"};
    public int updatesPort = 2229;
    public String updatesQueue = "updates";
    public String[] actionsAddresses = new String[]{"localhost"};
    public int actionsPort = 2229;
    public String actionsQueue = "actions";
    
    public String externalPubAddress = "localhost";
    public int externalPubPort = 1883;
    public String externalPubUser = null;
    public String externalPubPassword = null;
    public String externalPubClassName = "com.servioticy.dispatcher.publishers.MQTTPublisher";

    public String internalPubAddress = "localhost";
    public int internalPubPort = 1883;
    public String internalPubUser = null;
    public String internalPubPassword = null;
    public String internalPubClassName = "com.servioticy.dispatcher.publishers.MQTTPublisher";

    public String actionsPubAddress = "localhost";
    public int actionsPubPort = 1883;
    public String actionsPubUser = null;
    public String actionsPubPassword = null;
    public String actionsPubClassName = "com.servioticy.dispatcher.publishers.MQTTPublisher";

    public boolean benchmark = false;
    public String benchResultsDir = ".";

    public void loadConf(String path) {
    	HierarchicalConfiguration config;

    	try {
    		if (path == null) {
    			config = new XMLConfiguration(DispatcherContext.DEFAULT_CONFIG_PATH);
    		} else {
    			config = new XMLConfiguration(path);
    		}
    		config.setExpressionEngine(new XPathExpressionEngine());

    		this.restBaseURL = config.getString("servioticyAPI", this.restBaseURL);

    		ArrayList<String> updates = new ArrayList<String>();
    		if (config.containsKey("spouts/updates/addresses/address[1]")) {
    			for (int i = 1; config.containsKey("spouts/updates/addresses/address[" + i + "]"); i++) {
    				updates.add(config.getString("spouts/updates/addresses/address[" + i + "]"));
    			}
    		} else {
                for(String address: this.updatesAddresses){
                    updates.add(address);
                }
    		}
    		this.updatesAddresses = (String[]) updates.toArray(new String[]{});
    		this.updatesPort = config.getInt("spouts/updates/port", this.updatesPort);
    		this.updatesQueue = config.getString("spouts/updates/name", this.updatesQueue);

            ArrayList<String> actions= new ArrayList<String>();
            if (config.containsKey("spouts/actions/addresses/address[1]")) {
                for (int i = 1; config.containsKey("spouts/actions/addresses/address[" + i + "]"); i++) {
                    actions.add(config.getString("spouts/actions/addresses/address[" + i + "]"));
                }
            } else {
                for(String address: this.actionsAddresses){
                    actions.add(address);
                }
            }
            this.actionsAddresses = (String[]) actions.toArray(new String[]{});
            this.actionsPort = config.getInt("spouts/actions/port", this.actionsPort);
            this.actionsQueue = config.getString("spouts/actions/name", this.actionsQueue);

            this.benchmark = config.getBoolean("benchmark", this.benchmark);

            this.externalPubAddress = config.getString("publishers/external/address", this.externalPubAddress);
            this.externalPubPort = config.getInt("publishers/external/port", this.externalPubPort);
            this.externalPubUser = config.getString("publishers/external/username", this.externalPubUser);
            this.externalPubPassword = config.getString("publishers/external/password", this.externalPubPassword);
            this.externalPubClassName = config.getString("publishers/external/class", this.externalPubClassName);

            this.internalPubAddress = config.getString("publishers/internal/address", this.internalPubAddress);
            this.internalPubPort = config.getInt("publishers/internal/port", this.internalPubPort);
            this.internalPubUser = config.getString("publishers/internal/username", this.internalPubUser);
            this.internalPubPassword = config.getString("publishers/internal/password", this.internalPubPassword);
            this.internalPubClassName = config.getString("publishers/internal/class", this.internalPubClassName);

            this.actionsPubAddress = config.getString("publishers/actions/address", this.actionsPubAddress);
            this.actionsPubPort = config.getInt("publishers/actions/port", this.actionsPubPort);
            this.actionsPubUser = config.getString("publishers/actions/username", this.actionsPubUser);
            this.actionsPubPassword = config.getString("publishers/actions/password", this.actionsPubPassword);
            this.actionsPubClassName = config.getString("publishers/actions/class", this.actionsPubClassName);

            this.benchResultsDir = config.getString("benchResultsDir", this.benchResultsDir);

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
