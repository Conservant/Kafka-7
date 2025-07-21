package ru.mitenev.kafka;

public interface CommonConfig {

    String MESSAGES_TOPIC = "messages";
    String CONSUMER_GROUP = "my-consumer-group";
    String KAFKA_SERVERS = "localhost:9093,localhost:9094,localhost:9095";
}
