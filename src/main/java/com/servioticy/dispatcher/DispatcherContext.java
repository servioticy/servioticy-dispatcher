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
    public String[] kestrelAddresses = new String[]{"localhost"};
    public int kestrelPort = 2229;
    public String kestrelQueue = "services";
    public String kestrelQueueActions = "actions";
    
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

    		ArrayList<String> kestrel = new ArrayList<String>();
    		if (config.containsKey("kestrels/kestrel[1]/addr")) {
    			for (int i = 1; config.containsKey("kestrels/kestrel[" + i + "]/addr"); i++) {
    				kestrel.add(config.getString("kestrels/kestrel[" + i + "]/addr"));
    			}
    		} else {
    			kestrel.add(this.kestrelAddresses[0]);
    		}
    		this.kestrelAddresses = (String[]) kestrel.toArray(new String[]{});
    		this.kestrelPort = config.getInt("kestrels/port", this.kestrelPort);
    		this.kestrelQueue = config.getString("kestrels/queue", this.kestrelQueue);

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

            this.benchResultsDir = config.getString("benchResultsDir", this.benchResultsDir);

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
