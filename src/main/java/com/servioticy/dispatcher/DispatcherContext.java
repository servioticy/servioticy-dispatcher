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
import java.util.Map;
import java.util.Properties;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class DispatcherContext implements Serializable{
    private static final long serialVersionUID = 1L;
    static final public String DEFAULT_CONFIG_PATH = "dispatcher.xml";

    public String restBaseURL = "localhost";
    public String[] kestrelAddresses = new String[]{"localhost"};
    public int kestrelPort = 2229;
    public String kestrelQueue = "services";
    public Map<String, Properties> pubProperties;
    public Map<String, Properties> bootstrapsProperties;


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

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
