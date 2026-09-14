package com.orbitly.billing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Dead-Letter Queue consumer for invoice-events-dlq.
 *
 * MVP scope: logs the failed message for observability.
 * Production TODO: integrate with alerting (PagerDuty / Slack) and
 * persist to a dead_letter_events table for manual replay.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class DlqConsumer {

    @KafkaListener(
            topics  = "invoice-events.dlq",
            groupId = "orbitly-dlq-group"
    )
    public void consumeDlq(ConsumerRecord<String, String> record) {
        log.error("[DLQ] Failed invoice event after retries exhausted " +
                  "[key={}, partition={}, offset={}] payload={}",
                record.key(), record.partition(), record.offset(), record.value());
    }
}
