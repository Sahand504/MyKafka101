package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyProducer {
    private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("Hello world from producer!");

        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

        for(int i=0; i<10; i++) {
            for(int j=0; j<30; j++ ) {

                String topic = "test-topic";
                String key = "id_" + j;
                String value = "msg_" + j;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null) {
                            logger.info("partition: " + recordMetadata.partition());
                            logger.info("offset: " + recordMetadata.offset());
                            logger.info("key: " + key);
                        } else {
                            logger.error("Error while producing" + e);
                        }
                    }
                });
            }
            Thread.sleep(500);
        }

        // async send


        // For test and seeing result - send all data and block until it's done!
        producer.flush();

        producer.close();

    }

    static Properties getProperties() {
        Properties properties = new Properties();

        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9093");

        // serializer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }

}
