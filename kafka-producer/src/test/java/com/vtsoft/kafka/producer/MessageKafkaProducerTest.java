package com.vtsoft.kafka.producer;

import java.io.IOException;

import org.junit.Test;

public class MessageKafkaProducerTest {

	@Test
	public void test1() {

		try (MessageKafkaProducer kafkaProducer = new MessageKafkaProducer("test-topic")) {
			for (int i = 0; i < 1000000; i++) {
				kafkaProducer.sendMessage("Teste " + i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
