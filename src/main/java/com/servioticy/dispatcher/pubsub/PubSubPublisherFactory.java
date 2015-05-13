package com.servioticy.dispatcher.pubsub;

import java.net.UnknownHostException;


public class PubSubPublisherFactory {
	
	public static PublisherInterface getPublisher(String uri, String taskId) throws Exception, UnknownHostException, InterruptedException{
		
		//return (PublisherInterface)(new SRPublisher(subscriptionString, taskId));
		return (PublisherInterface)(new MQTTPublisher(uri, taskId));
	}

}