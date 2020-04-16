package com.github.simplestep.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final org.slf4j.Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstarpServers = "127.0.0.1:9092";
        // create producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstarpServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            //create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key ,value);

            logger.info("key:" + key);

            //send data - asynchronous

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or execute is throen

                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n"
                                + "Partitions" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());

                        //the record was successfully
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();
        }
        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
