package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        System.out.println("Hello world!");
        Properties properties  = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i =0; i < 10;i++) {

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "hello world "+i);

            //async
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metaData, Exception e) {
                    //executes every time a rec is sucessfully sent or an exception is thrown
                    if (e == null) {
                        log.info("Received new metadata / \n" +
                                "Topic: " + metaData.topic() + "\n" +
                                "Partition: " + metaData.partition() + "\n" +
                                "Offset: " + metaData.offset() + "\n" +
                                "Timestamp: " + metaData.timestamp() + "\n"

                        );
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        //syn
        producer.flush();

        //flush and close
        producer.close();
    }
}