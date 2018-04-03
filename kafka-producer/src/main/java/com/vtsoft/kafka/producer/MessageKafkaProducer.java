package com.vtsoft.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageKafkaProducer {

	private final KafkaProducer<String, String> producer;
	private final String topic;

	public MessageKafkaProducer(String topic) {
		Properties properties = createProducerProperties();
		this.producer = new KafkaProducer<String, String>(properties);
		this.topic = topic;
	}

	private Properties createProducerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	public void sendMessage(String message) {
		producer.send(new ProducerRecord<String, String>(this.topic, "message", message));
		producer.close();
	}
}
