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
package com.servioticy.dispatcher.publishers;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

public class MQTTPublisher extends Publisher {
	//private IMqttToken token;

	private IMqttAsyncClient asyncClient = null;

	public MQTTPublisher(String address, Integer port, String pubId) throws Exception{
			asyncClient = new MqttAsyncClient(address + ":" + port, pubId);
	}

	@Override
	public void connect(String username, String password) throws Exception {
	
		MqttConnectOptions options = new MqttConnectOptions();
		String uris[] = new String[1];
		uris[0] = asyncClient.getServerURI();
		options.setServerURIs(uris);
		options.setUserName(username);
		options.setPassword(password.toCharArray());
		
		LOG.debug("MQTT connect options: "+options.toString());
		
		try {
			asyncClient.connect(options).waitForCompletion();
		} catch (MqttSecurityException e) {
			LOG.error("FAIL in connect: ", e);
            throw e;
		} catch (MqttException e) {
			LOG.error("FAIL in connect:", e);
            throw e;
		}	
	}

	@Override
	public void close() {
		try {
			asyncClient.disconnect().waitForCompletion();
		} catch (MqttException e) {
			LOG.error("FAIL", e);
		}
	}

	@Override
	public boolean isConnected() {
		return asyncClient.isConnected();
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
