package com.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
/**
* Demo to check :: Messages with same key goes to the same partition.
* */
public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) {
        log.info("----------- Starting Producer With Keys ---------");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // crate producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a producer record
        for(int i = 0 ; i<10 ;i++) {
            String topic = "demo-topic-1";
            String key = "Message_key01";
            /** uncomment below line to check messages with different keys goes to different partitions*/
//            String key = "Message_key"+i;
            String value = "value_"+i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);


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
