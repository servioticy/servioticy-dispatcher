package com.servioticy.dispatcher.pubsub;

public interface PublisherInterface {
	
	public void connect(String uris[], String username, String password);
	public void close();
	public void publishMessage(String topic, String message);
	
}
