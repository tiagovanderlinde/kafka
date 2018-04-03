package com.vtsoft.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageKafkaConsumer {

	private final KafkaConsumer<String, String> consumer;
	private final String topic;

	public MessageKafkaConsumer(String topic) {
		Properties properties = createConsumerProperties();
		this.consumer = new KafkaConsumer<String, String>(properties);
		this.topic = topic;
		this.consumer.subscribe(Arrays.asList(this.topic));
	}

	private Properties createConsumerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("group.id", "teste");
		properties.put("enable.auto.commit", "false");
		properties.put("session.timeout.ms", "30000");
		properties.put("auto.offset.reset", "earliest");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return properties;
	}

	public void readMessages() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
						+ ", Offset: " + record.offset());
			}
			consumer.commitSync();
		}
	}
}
