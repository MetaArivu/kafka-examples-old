package com.metamagic.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemo {

	public static void main(String[] args) {
		Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9093,127.0.0.1:9094");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "mygroup1");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("first_topic_1"));

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(10);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Partition: " + consumerRecord.partition() +
                                    ", Offset: " + consumerRecord.offset() +
                                    ", Key: " + consumerRecord.key() +
                                    ", Value: " + consumerRecord.value()+
                                    ", timestamp: "+consumerRecord.timestamp());

            }
            kafkaConsumer.commitSync();
        }
		
	}
	
	
	
}
