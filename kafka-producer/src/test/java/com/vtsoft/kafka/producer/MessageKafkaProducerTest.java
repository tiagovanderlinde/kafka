package com.vtsoft.kafka.producer;

import org.junit.Test;

public class MessageKafkaProducerTest {

	@Test
	public void test1() {

		for (int i = 0; i < 100; i++) {
			MessageKafkaProducer kafkaProducer = new MessageKafkaProducer("test-topic");
			kafkaProducer.sendMessage("Teste " + i);
		}
	}
}
