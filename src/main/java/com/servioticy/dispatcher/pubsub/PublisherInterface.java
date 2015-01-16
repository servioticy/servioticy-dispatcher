package com.servioticy.dispatcher.pubsub;

public interface PublisherInterface {
	
	public void connect(String uri, String username, String password);
	public void close();
	public boolean isConnected();
	public void publishMessage(String topic, String message);
	
}
