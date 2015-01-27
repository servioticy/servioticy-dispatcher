package com.servioticy.dispatcher.pubsub;

import java.net.UnknownHostException;


public class PubSubPublisherFactory {
	public static final int PUB_CSB = 0;
	public static final int PUB_MQTT = 1;

	public static PublisherInterface getPublisher(String uri, String taskId, int type) throws Exception, UnknownHostException, InterruptedException{
		switch(type){
			case PUB_CSB:
				return (PublisherInterface)(new CSBPublisher(uri, taskId));
			case PUB_MQTT:
			default:
				return (PublisherInterface)(new MQTTPublisher(uri, taskId));
		}
	}

}
