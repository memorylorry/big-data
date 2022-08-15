package com.github.memorylorry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AutoOffsetConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "n1:9092");
		props.setProperty("group.id", "test");
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test"));
		final int minBatchSize = 200;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
				System.out.printf("partitionID = %s offset = %d, key = %s, value = %s, ts = %s%n", record.partition(), record.offset(), record.key(), record.value(), sdf.format(record.timestamp()));
			}
			if (buffer.size() >= minBatchSize) {
//				insertIntoDb(buffer);
				consumer.commitSync();

//				buffer.clear();
			}
		}
	}
}
