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

    public static String restBaseURL = "localhost";
    public static String[] kestrelAddresses = new String[]{"localhost"};
    public static int kestrelPort = 2229;
    public static String kestrelQueue = "services";
    public static Map<String, Properties> pubProperties;
    public static Map<String, Properties> bootstrapsProperties;


    public static void loadConf(String path) {

        HierarchicalConfiguration config;

        try {
            if (path == null) {
                config = new XMLConfiguration(DispatcherContext.class.getResource(DispatcherContext.DEFAULT_CONFIG_PATH));
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

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
