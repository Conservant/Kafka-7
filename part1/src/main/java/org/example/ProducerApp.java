package org.example;

import java.util.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.*;

public class ProducerApp {

    public static void main(String[] args) {

        int MSG_COUNT = 5;
        String HOST = "rc1a-t8a7qhknf9v480a7.mdb.yandexcloud.net:9091,rc1b-aq1r1qodp5g4i29m.mdb.yandexcloud.net:9091,rc1d-tqk6jnptp228kr4l.mdb.yandexcloud.net:9091";
        String TOPIC = "my_super_topic";
        String USER = "kafka-test-user";
        String PASS = "kafka-test-pass";
        String TS_FILE = "/etc/security/ssl";
        String TS_PASS = "super-pass";

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, USER, PASS);
        String KEY = "key";

        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("acks", "all");
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", jaasCfg);
        props.put("ssl.truststore.location", TS_FILE);
        props.put("ssl.truststore.password", TS_PASS);

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 1; i <= MSG_COUNT; i++){
                producer.send(new ProducerRecord<String, String>(TOPIC, KEY, "test message")).get();
                System.out.println("{\"message\": \"hi user"+ i + "\", \"messageFrom\": \"admin\", \"messageTo\": \"user"+i+"\"");
            }
            producer.flush();
            producer.close();
        } catch (Exception ex) {
            System.out.println(ex);
            producer.close();
        }
    }
}