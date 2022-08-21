package com.github.memorylorry.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


class Consumer implements Runnable{
    private String id;
    KafkaConsumer<String, String> consumer;
    public Consumer(String id){
        this.id = id;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "n2:9092,n3:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);

//        consumer.subscribe(Arrays.asList("test"));
        List<TopicPartition> partitions = new ArrayList<>();
        for(int i=0;i<3;i++)
            partitions.add(new TopicPartition("test", i));
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    @Override
    public void run() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("consumerID = %s offset = %d, key = %s, value = %s, ts = %s%n", id, record.offset(), record.key(), record.value(), sdf.format(record.timestamp()));
        }
    }
}

public class Test {
    public static void main(String[] args) {
        for(int i=1;i<3;i++){
            new Thread(new Consumer(String.valueOf(i))).start();
        }
    }
}
