package com.github.simplestep.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){

    }
    private   void run(){
        org.slf4j.Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());


        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-seven-applications";
        String topic="first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating Consumer");

        Runnable myConsumerThread = new ConsumerThread(
                bootstrapServer,
                groupId,
                topic,
                latch
        );
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{

            logger.info("Cought shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("application interrupt", e);
        }
        finally {
            logger.info("application closing");
        }

    }
    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;


        private org.slf4j.Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public ConsumerThread(String bootstrapServer,String groupId, String topic,CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {


            while (true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> records1 : records){
                    logger.info("Key:" +records1.key() + ", Value" + records1.value());
                    logger.info("partitions:" + records1.partition() + "offset" + records1.offset());
                }
            }
            }catch (WakeupException e){
                logger.info("received shut down ");
            }finally {
                consumer.close();
                latch.countDown();
            }

        }
        public void shutdown(){
            consumer.wakeup();
        }
    }
}
