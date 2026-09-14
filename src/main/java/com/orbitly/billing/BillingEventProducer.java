package com.orbitly.billing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class BillingEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${orbitly.kafka.topic.billing-events}")
    private String topic;

    /**
     * Publishes a billing event to Kafka.
     *
     * @param eventId  Stripe event ID — used as the message key so that events
     *                 for the same Stripe object always land on the same partition.
     * @param payload  Raw Stripe JSON event payload.
     */
    public void publish(String eventId, String payload) {
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, eventId, payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish billing event [eventId={}]: {}", eventId, ex.getMessage());
            } else {
                log.info("Published billing event [eventId={}, partition={}, offset={}]",
                        eventId,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
