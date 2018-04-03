package com.vtsoft.kafka.consumer;

import org.junit.Test;

public class MessageKafkaConsumerTest {

	@Test
	public void test1() {

		MessageKafkaConsumer kafkaProducer = new MessageKafkaConsumer("test-topic");
		kafkaProducer.readMessages();
	}
}
