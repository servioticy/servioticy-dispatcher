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
package com.servioticy.dispatcher.pubsub;

import com.ibm.csb.client.CSBFactory;
import com.ibm.csb.client.Session;
import org.apache.log4j.Logger;

/**
 * Created by alvaro on 26/01/15.
 */
public class CSBPublisher implements PublisherInterface {

    private static Logger LOG = org.apache.log4j.Logger.getLogger(CSBPublisher.class);
    private Session session;
    @Override
    public void connect(String uri, String username, String password) {

            final CSBFactory factory;
            try {
                factory = CSBFactory.getInstance();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            Session session = null;
            try {
                session = factory.createSession("abiell.pc.ac.upc.edu",1033,null);
            } catch (Exception e) {
                e.printStackTrace();
            }

    }

    @Override
    public void close() {

    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void publishMessage(String topic, String message) {

    }
}
