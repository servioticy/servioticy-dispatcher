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

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;


/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class MQTTPublisher implements PublisherInterface {

	
	//private IMqttToken token;
	private IMqttAsyncClient asyncClient = null; 

	
	public MQTTPublisher(String pubId){  
		
		
		try {
			
			asyncClient = new MqttAsyncClient("tcp://api.servioticy.com:1883",pubId); 
						
			
		} catch (MqttSecurityException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}
	     
	}
	
	private void connect() {
	
		String uris[] = {"tcp://api.servioticy.com:1883"};
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURIs(uris);
		options.setUserName("compose");
		options.setPassword("shines".toCharArray());
		
		try {
			asyncClient.connect(options).waitForCompletion();
		} catch (MqttSecurityException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}	
	}
	
	public void close() {
		try {
			asyncClient.disconnect().waitForCompletion();
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}
	
	public void publishMessage(String topic, String msg) {
		int attempts = 0;
		if(asyncClient != null) {
			while(!asyncClient.isConnected() && attempts < 10) { 
				this.connect();
			}
			if(!asyncClient.isConnected())
				return;
			
			try {
				asyncClient.publish(topic, new MqttMessage(msg.getBytes())).waitForCompletion();
			} catch (MqttException e) {
				e.printStackTrace();
			}
			
		}
		
	}

	

}
