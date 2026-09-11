package com.orbitly.billing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${orbitly.kafka.topic.invoice-events}")
    private String topic;

    /**
     * Publishes an invoice event to Kafka.
     * Keyed by invoiceId — all events for the same invoice land on the same partition,
     * preserving ordering for the idempotent consumer.
     */
    public void publishInvoiceCreated(UUID invoiceId, long amountCents, String currency) {
        InvoiceEventMessage message = new InvoiceEventMessage(
                UUID.randomUUID().toString(),    // eventId — consumer uses this for idempotency
                invoiceId.toString(),
                "invoice.created",
                amountCents,
                currency,
                OffsetDateTime.now()
        );

        publish(invoiceId.toString(), message);
    }

    private void publish(String key, InvoiceEventMessage message) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize InvoiceEventMessage: {}", e.getMessage());
            return;
        }

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, key, payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish invoice event [invoiceId={}, eventType={}]: {}",
                        key, message.eventType(), ex.getMessage());
            } else {
                log.info("Published invoice event [invoiceId={}, eventType={}, partition={}, offset={}]",
                        key, message.eventType(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
