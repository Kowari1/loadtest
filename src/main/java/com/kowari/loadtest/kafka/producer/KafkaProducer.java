package com.kowari.loadtest.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Component
public class KafkaProducer {

    private static final String TOPIC = "test-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendBatch(List<String> batch) {

        try {
            String message = objectMapper.writeValueAsString(batch);

            kafkaTemplate.send(TOPIC, message).get();

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to send batch to Kafka", e
            );
        }
    }
}
