package com.orbitly.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${orbitly.kafka.topic.billing-events}")
    private String billingEventsTopic;

    /**
     * Declares the billing-events topic.
     * Spring Kafka's KafkaAdmin will create it on startup if it doesn't exist.
     */
    @Bean
    public NewTopic billingEventsTopic() {
        return TopicBuilder.name(billingEventsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
