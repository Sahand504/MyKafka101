package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MyConsumer.class);

    public static void main(String[] args) {
        logger.info("Hello world from consumer!");

        String topic = "test-topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());

        // get reference to the main thread
        final Thread mainTread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Shutdown detected...");

                logger.info("calling wakeup to throw wakeup exception...");
                consumer.wakeup();

                // join the main thread to allow execution of the code
                try {
                    mainTread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
//                logger.info("Polling records from the consumer...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("partition: {} | offset: {} | key: {} | value: {}", record.partition(), record.offset(), record.key(), record.value());

                }
            }
        } catch (WakeupException wakeupException) {
            logger.info("Consumer is starting to shutdown!");
        } catch (Exception e) {
            logger.error("Unexpected exception!", e);
        } finally {
            consumer.close();
            logger.info("Consumer is now gracefully shutdown!");
        }

    }

    static Properties getProperties() {
        Properties properties = new Properties();

        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9093");

        // serializer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "my-consumer-group-1");
        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        return properties;
    }
}
