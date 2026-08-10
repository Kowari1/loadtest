package com.kowari.loadtest.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
public class KafkaMessageConsumer {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;

    private final ObjectMapper objectMapper;

    private final KafkaConsumer<String, String> consumer;
    private final TopicPartition partition;

    public KafkaMessageConsumer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            ObjectMapper objectMapper) {

        this.objectMapper = objectMapper;
        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "data-consumer"
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        consumer = new KafkaConsumer<>(properties);

        partition = new TopicPartition(TOPIC, PARTITION);

        consumer.assign(List.of(partition));
    }

    public synchronized List<String> readNewMessages() {

        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(500));

        List<String> result = new ArrayList<>();

        try {
            for (var record : records) {
                List<String> batch =
                        objectMapper.readValue(record.value(), List.class);

                result.addAll(batch);
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
            }

            return result;

        } catch (Exception e) {
            throw new RuntimeException("Failed to read Kafka messages", e);
        }
    }
}
