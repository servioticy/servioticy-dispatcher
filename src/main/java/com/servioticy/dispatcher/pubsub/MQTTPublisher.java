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

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

import com.servioticy.dispatcher.bolts.PubSubDispatcherBolt;


/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class MQTTPublisher implements PublisherInterface {

	private static Logger LOG = org.apache.log4j.Logger.getLogger(MQTTPublisher.class);
	
	//private IMqttToken token;
	private IMqttAsyncClient asyncClient = null; 

	
	public MQTTPublisher(String uri, String pubId){  
		
		
		try {
			
			//asyncClient = new MqttAsyncClient("tcp://api.servioticy.com:1883",pubId);
			asyncClient = new MqttAsyncClient(uri, pubId);
			//asyncClient = new MqttAsyncClient("tcp://192.168.56.101:1883",pubId);
						
			
		} catch (MqttSecurityException e) {
			LOG.error("FAIL in constructor: ", e);
		} catch (MqttException e) {
			LOG.error("FAIL in constructor: ", e);
		}
	     
	}
	
	public void connect(String uris[], String username, String password) {
	
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURIs(uris);
		options.setUserName(username);
		options.setPassword(password.toCharArray());
		
		LOG.debug("MQTT connect options: "+options.toString());
		
		try {
			asyncClient.connect(options).waitForCompletion();
		} catch (MqttSecurityException e) {
			LOG.error("FAIL in connect: ", e);
		} catch (MqttException e) {
			LOG.error("FAIL in connect:", e);
		}	
	}
	
	public void close() {
		try {
			asyncClient.disconnect().waitForCompletion();
		} catch (MqttException e) {
			LOG.error("FAIL", e);
		}
	}
	
	public void publishMessage(String topic, String msg) {
		int attempts = 0;
		if(asyncClient != null) {
			
			if(!asyncClient.isConnected())
				return;
			
			try {
				if(msg == null){
					LOG.error("FAIL in publishMessage: msg is null");
					return;
				}
				asyncClient.publish(topic, new MqttMessage(msg.getBytes()));//.waitForCompletion();
			} catch (Exception e) {
				LOG.error("FAIL in publishMessage: ", e);
			}
			
		}
		
	}

	

}
