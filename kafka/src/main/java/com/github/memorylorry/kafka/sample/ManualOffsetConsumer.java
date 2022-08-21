package com.github.memorylorry.kafka.sample;

import com.github.memorylorry.kafka.util.KafkaOffsetManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.List;

public class ManualOffsetConsumer {
	public static void main(String[] args) {
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "n1:9092,n2:9092,n3:9092");
		conf.setProperty("group.id", "test");
		conf.setProperty("auto.offset.reset", "earliest");
		conf.setProperty("enable.auto.commit", "false");
		conf.setProperty("auto.commit.interval.ms", "1000");
		conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(conf);
		String topic = "test";

		List<KafkaOffsetManager> managers = new ArrayList<>();
		List<TopicPartition> partitions = new ArrayList<>();
		for(int i=0; i<3; i++){
			managers.add(new KafkaOffsetManager(String.format("kafka_%d", i)));
			partitions.add(new TopicPartition(topic, i));
		}

		consumer.assign(partitions);

		for(int i=0; i<3; i++){
			long offset = managers.get(i).readOffset();
			consumer.seek(partitions.get(i), offset+1);
			System.out.println(String.format("s_%d %s", i, offset));
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long m0 = -1, m1 = -1, m2 = -1;
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("partitionID = %s offset = %d, key = %s, value = %s, ts = %s%n", record.partition(), record.offset(), record.key(), record.value(), sdf.format(record.timestamp()));
				switch (record.partition()){
					case 0:
						m0 = record.offset();
						break;
					case 1:
						m1 = record.offset();
						break;
					case 2:
						m2 = record.offset();
						break;
				}
			}
			consumer.commitSync();
			// offset save
			if(m0>0) managers.get(0).write(m0);
			if(m1>0) managers.get(1).write(m1);
			if(m2>0) managers.get(2).write(m2);
		}
		
		
	}
}
