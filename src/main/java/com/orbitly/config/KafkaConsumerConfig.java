package com.orbitly.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    /**
     * Error handler for invoice-events consumer:
     *  - Retries 3 times with 1 s fixed back-off
     *  - After retries exhausted, sends record to invoice-events-dlq via DeadLetterPublishingRecoverer
     */
    @Bean
    public CommonErrorHandler invoiceEventsErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
                    log.error("Publishing failed record to DLQ [topic={}, key={}, error={}]",
                            record.topic() + ".dlq", record.key(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition(
                            record.topic() + ".dlq", 0);
                });

        // 3 retries, 1 000 ms between attempts
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
    }
}
