package com.servioticy.dispatcher.pubsub;

public class MqttTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MQTTPublisher publisher = new MQTTPublisher("test");
		System.out.println("Publishing");
		publisher.publishMessage("foo/to", "HELLO WORLD");
		System.out.println("Done");
	}

}
