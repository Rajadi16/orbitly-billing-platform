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

    @Value("${orbitly.kafka.topic.invoice-events}")
    private String invoiceEventsTopic;

    @Bean
    public NewTopic billingEventsTopic() {
        return TopicBuilder.name(billingEventsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * invoice-events: 3 partitions so events for different invoices
     * can be processed in parallel while preserving per-invoice order (keyed by invoiceId).
     */
    @Bean
    public NewTopic invoiceEventsTopic() {
        return TopicBuilder.name(invoiceEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
