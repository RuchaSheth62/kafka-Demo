package com.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
/**
 * Demo to check metadata of record to verify if record is successfully received by consumer or not.
 * **/
public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        log.info("----------- Starting Producer With Callbacks ---------");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // crate producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a producer record
        for(int i = 0 ; i<10 ;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("demo-topic-1", "record-" + i);


            // send the data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or exception is thrown
                    if (e == null) {
                        log.info("Received new metadata" +
                                "\nTopic: " + recordMetadata.topic() +
                                "\nPartition: " + recordMetadata.partition() +
                                "\nOffset:" + recordMetadata.offset() +
                                "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }
        // flush data - synchronous - tells producers to actually send the data and block until its done
        producer.flush();

        // close the Producer
        producer.close();
    }
}
