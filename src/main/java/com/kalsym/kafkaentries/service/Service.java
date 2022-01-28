package com.kalsym.kafkaentries.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author hasan
 */
@org.springframework.stereotype.Service
public class Service {

    private static final String TOPIC = "topic";
    private static final String GROUP = "demoGroup";
    private static final int RECORDS = 50000000;
    private final Properties producerProps = new Properties();
    private final Properties consumerProps = new Properties();
    private KafkaProducer<String, byte[]> producer;
    
    public String insertValues() throws InterruptedException {
        int i = 0;
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("kafka.topic.name", TOPIC);
        producer = new KafkaProducer<>(this.producerProps, new StringSerializer(), new ByteArraySerializer());
        for (; i < RECORDS; i++) {
            byte[] payload = ("Value# " + i).getBytes();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(producerProps.getProperty("kafka.topic.name"), payload);
            producer.send(record);
            
        }
        producer.close();
        return i + " values inserted";
    }

    public List<String> fetchValues() throws InterruptedException {
        List<String> allRecords = new ArrayList<>();

        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", GROUP);
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("max.poll.records", "50000000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try ( KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);) {
            consumer.subscribe(Arrays.asList(TOPIC));

            for (int i = 0; i < 5000000; i++) {
                ConsumerRecords<String, String> records = consumer.poll(0);
                for (ConsumerRecord<String, String> record : records) {
                    allRecords.add(record.value());
//                    System.out.println("Fetching Record At Index = "+ i +" Value = " + record.value());
                }

            }
            System.out.println("Finshed Fetching!");
            consumer.close();
        }
        return allRecords;
    }
}
