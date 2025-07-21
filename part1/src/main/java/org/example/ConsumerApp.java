package org.example;

import java.time.Duration;
import java.util.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.*;

public class ConsumerApp {

    public static void main(String[] args) {

        String HOST = "rc1a-t8a7qhknf9v480a7.mdb.yandexcloud.net:9091,rc1b-aq1r1qodp5g4i29m.mdb.yandexcloud.net:9091,rc1d-tqk6jnptp228kr4l.mdb.yandexcloud.net:9091";;
        String TOPIC = "my_super_topic";
        String USER = "kafka-test-user";
        String PASS = "kafka-test-pass";
        String TS_FILE = "/etc/security/ssl";
        String TS_PASS = "super-pass";

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, USER, PASS);
        String GROUP = "demo";

        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("group.id", GROUP);
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", jaasCfg);
        props.put("ssl.truststore.location", TS_FILE);
        props.put("ssl.truststore.password", TS_PASS);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(new String[] {TOPIC}));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + ":" + record.value());
            }
        }
    }
}