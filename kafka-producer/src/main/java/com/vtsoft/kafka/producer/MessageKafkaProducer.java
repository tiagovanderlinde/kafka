package com.vtsoft.kafka.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageKafkaProducer implements Closeable {

	private final KafkaProducer<String, String> producer;
	private final String topic;

	public MessageKafkaProducer(String topic) {
		Properties properties = createProducerProperties();
		this.producer = new KafkaProducer<String, String>(properties);
		this.topic = topic;
	}

	private Properties createProducerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
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
		
		producer.send(new ProducerRecord<String, String>(this.topic, "message", message), (metadata, e) -> {
		    if (e != null) {
		      e.printStackTrace();
		    }
		    System.out.println("Sent:" + message + ", Offset: " + metadata.offset());
		  });
	}

	@Override
	public void close() throws IOException {
		producer.close();
	}
}
