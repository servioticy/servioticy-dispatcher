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

import com.ibm.sr.OutgoingObjectMessage;
import com.ibm.sr.SR;
import com.ibm.sr.SRFactory;
import com.ibm.sr.config.ReliabilityMode;
import com.ibm.sr.config.SRConfig;
import com.ibm.sr.exceptions.IllegalConfigException;
import com.ibm.sr.exceptions.SRException;
import com.ibm.sr.pubsub.Topic;
import com.ibm.sr.pubsub.TopicConfig;
import com.ibm.sr.pubsub.TopicPublisher;
import com.ibm.sr.pubsub.TopicPublisherConfig;
import com.ibm.sr.samples.PrintOnlySREventListener;
import com.ibm.sr.samples.basic.InMemoryAddresResolver;
import com.ibm.sr.samples.basic.PrintOnlyTopicPublisherEventListner;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class SRPublisher {
	private Boolean reliable;
	private Topic topic;
	private SR sr;
	private TopicPublisher publisher;

	public SRPublisher(String topicName, String pubId) throws IllegalConfigException, UnknownHostException, SRException, InterruptedException{

        Set<String> bootstrapSet = new LinkedHashSet<String>();


        int basePort = 43210;
        // Get computer name:
		// TODO This should be done once in the bolt class.
		String computerName = InetAddress.getLocalHost().getHostName();
		
		String selfAddress = DispatcherContext.pubProperties.get(computerName).getProperty(DispatcherContext.PUBLISHER_LOCAL_ADDRESS);
		String extAddress = DispatcherContext.pubProperties.get(computerName).getProperty(DispatcherContext.PUBLISHER_EXTERNAL_ADDRESS);
		int extPort = Integer.parseInt(DispatcherContext.pubProperties.get(computerName).getProperty(DispatcherContext.PUBLISHER_EXTERNAL_PORT));
		
		this.reliable = false;
		
		InMemoryAddresResolver addressResolver = new InMemoryAddresResolver();
		for(Entry<String, Properties> props: DispatcherContext.bootstrapsProperties.entrySet()){
			bootstrapSet.add(props.getKey());
			addressResolver.put(props.getKey(), props.getValue().getProperty(DispatcherContext.BOOTSTRAP_ADDRESS), 
			Integer.parseInt(props.getValue().getProperty(DispatcherContext.BOOTSTRAP_PORT)));
		}
		
		SRFactory factory = SRFactory.getInstance();
		
		// TODO In the future this will be useful
//		while(!available(basePort) && basePort < 50000){
//			basePort++;
//		}
		
		if(basePort >= 50000){
			throw new SRException();
		}
		
		this.sr = createSR(basePort, extPort, bootstrapSet, addressResolver, factory, selfAddress, extAddress, pubId);
		Thread.sleep(1000);
        TopicConfig topicConfig = factory.createTopicConfig(new Properties());
        this.topic = sr.createTopic(topicName, topicConfig);
		
		Properties topicPublisherConfigProperties = new Properties();
		if (this.reliable)
        {
            topicPublisherConfigProperties.setProperty(
                    TopicPublisherConfig.RELIABILITY_MODE_KEY,
                    ReliabilityMode.RELIABLE_ORDERED_ACK_BASED.toString());
            topicPublisherConfigProperties.setProperty(
                    TopicPublisherConfig.HEARTBEAT_MAX_INTERVAL_MILLIS_KEY,
                    "5000");
            topicPublisherConfigProperties.setProperty(
                    TopicPublisherConfig.HEARTBEAT_NUM_CAN_MISS_KEY, "12");
        }
        else
        {
            // Best effort
            topicPublisherConfigProperties.setProperty(
                    TopicPublisherConfig.RELIABILITY_MODE_KEY,
                    ReliabilityMode.UNRELIABLE_UNORDERED.toString());
        }
		
		TopicPublisherConfig publisherConfig = factory
                .createTopicPublisherConfig(topicPublisherConfigProperties);

		this.publisher = this.sr.createTopicPublisher(publisherConfig, 
				new PrintOnlyTopicPublisherEventListner(sr
						.getNodeId().getName(), topic.getTopicName()), 
				topic);
		
	}
	
	public void close() throws SRException{
//		this.publisher.close();
		System.err.println("CLOSING");
		this.sr.close();
		System.err.println("CLOSED");
	}
	
	public void publishMessage(String msg) throws SRException{
		OutgoingObjectMessage message;
		message = sr.createOutgoingObjectMessage();
		message.setObject(msg);
		
		this.publisher.publishMessage(message);
	}
	
	public String getTopicName(){
		return this.topic == null ? null : topic.getTopicName();
	}
	
    /**
     * @param port
     * @param srArray
     * @param bootstrap_set
     * @param addressResolver
     * @param factory
     * @param i
     * @throws SRException
     * @throws IllegalConfigException
     * @throws UnknownHostException
     */
	private static SR createSR(int selfPort, int extPort, Set<String> bootstrap_set,
            InMemoryAddresResolver addressResolver, SRFactory factory, String selfAddress, String extAddress, String pubId)
            throws SRException, IllegalConfigException, UnknownHostException
    {
        SRConfig config;
        String nodeName = "pub" + pubId;
        Properties prop = new Properties();
        prop.setProperty(SRConfig.NODE_NAME_KEY, nodeName);
        prop.setProperty(SRConfig.BUS_NAME_KEY, "bus");
        prop.setProperty(SRConfig.NETWORK_INTERFACE_KEY, selfAddress);
        prop.setProperty(SRConfig.TCP_RECEIVER_PORT_KEY, String.valueOf(selfPort));

        config = factory.createSRConfig(prop);
        addressResolver.put(nodeName, extAddress, extPort);

        try
        {
            SR sr = factory.createSR(config, new PrintOnlySREventListener(
                    nodeName), addressResolver, bootstrap_set);

            return sr;
        }
        catch (SRException e)
        {
            throw new SRException("Error creating SR", e);
        }
    }
    
    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     */
    private static boolean available(int port) {
//        if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
//            throw new IllegalArgumentException("Invalid start port: " + port);
//        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
}
