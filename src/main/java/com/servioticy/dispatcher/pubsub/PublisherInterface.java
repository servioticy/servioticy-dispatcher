package com.servioticy.dispatcher.pubsub;

public interface PublisherInterface {

	public void close();
	public void publishMessage(String topic, String message);
	
}
