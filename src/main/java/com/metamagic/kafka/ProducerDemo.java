package com.metamagic.kafka;

import java.util.Calendar;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	
	public static void main(String []arg){
		Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9093,127.0.0.1:9094");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        
        for (int key=0; key < 10; key++){
        	String msg = "Hello its  " + Calendar.getInstance().getTimeInMillis()+" "+Integer.toString(key);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic_1",msg);
            producer.send(producerRecord);
            //producer.send(producerRecord,new KafkaDemoCallBack(msg));
        }



        producer.close();
		
	}
	
	
}

class KafkaDemoCallBack implements Callback {

    private final String message;

    public KafkaDemoCallBack(String message) {
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
        	System.out.println(this.message+" - sent and callback called.");
        } else {
            exception.printStackTrace();
        }
    }
}
