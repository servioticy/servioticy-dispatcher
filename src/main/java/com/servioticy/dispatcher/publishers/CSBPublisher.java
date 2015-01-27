/*******************************************************************************
 * Copyright 2015 Barcelona Supercomputing Center (BSC)
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
package com.servioticy.dispatcher.publishers;

import com.ibm.csb.client.*;
import org.apache.log4j.Logger;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class CSBPublisher extends Publisher {

    private static Logger LOG = org.apache.log4j.Logger.getLogger(CSBPublisher.class);
    private String address;
    private int port;
    private Session session;
    private final CSBFactory factory;
    private TopicPublisher tp;

    protected CSBPublisher(String address, int port, String pubId) throws Exception{
            this.factory = CSBFactory.getInstance();
            this.address = address;
            this.port = port;
    }
    @Override
    public void connect(String username, String password) throws Exception{
        this.session = factory.createSession(address, port, null);
    }

    @Override
    public void close() throws Exception{
        this.session.close();
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void publishMessage(String topicStr, String message) throws Exception{
        Topic topic = session.createTopic(topicStr, null);
        TopicPublisher tp = session.createTopicPublisher(topic, new PubSubEventListener() {
            @Override
            public void onEvent(PubSubEvent event) {

            }
        }, null);
        Message msg = factory.createMessage();
        msg.setBuffer((message).getBytes());
        tp.publish(msg);
    }
}
