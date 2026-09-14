package com.orbitly.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.Invoice;
import com.orbitly.invoice.InvoiceEvent;
import com.orbitly.invoice.InvoiceEventRepository;
import com.orbitly.invoice.InvoiceRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class InvoiceEventConsumer {

    private final InvoiceRepository      invoiceRepository;
    private final InvoiceEventRepository invoiceEventRepository;
    private final ObjectMapper           objectMapper;

    /**
     * Idempotent consumer for the invoice-events topic.
     *
     * Idempotency: before processing, check if invoice_events already contains
     * a row with this eventId. If yes → skip + ack (prevents duplicate processing
     * even after consumer restart or partition rebalance).
     *
     * Error handling: DefaultErrorHandler (see KafkaConsumerConfig) retries 3x
     * with 1 s back-off, then routes to invoice-events-dlq via
     * DeadLetterPublishingRecoverer.
     */
    @KafkaListener(
            topics  = "${orbitly.kafka.topic.invoice-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String rawPayload = record.value();

        InvoiceEventMessage message;
        try {
            message = objectMapper.readValue(rawPayload, InvoiceEventMessage.class);
        } catch (Exception e) {
            log.error("Unparseable invoice event at offset={}: {}", record.offset(), e.getMessage());
            ack.acknowledge();
            return;
        }

        // ── Idempotency guard ────────────────────────────────────────────────
        if (invoiceEventRepository.existsByEventId(message.eventId())) {
            log.info("Duplicate eventId={} — skipping", message.eventId());
            ack.acknowledge();
            return;
        }

        log.info("Processing invoice event [eventId={}, type={}, invoiceId={}]",
                message.eventId(), message.eventType(), message.invoiceId());

        process(message, rawPayload, record.offset());
        ack.acknowledge();
    }

    // ── Processing ───────────────────────────────────────────────────────────

    private void process(InvoiceEventMessage message, String rawPayload, long offset) {
        Invoice invoice = invoiceRepository.findById(
                java.util.UUID.fromString(message.invoiceId()))
                .orElseThrow(() -> new IllegalStateException(
                        "Invoice not found for eventId=" + message.eventId()));

        // MVP scope: log confirmation; extend here for downstream actions
        log.info("Invoice [id={}, status={}] confirmed via event type='{}'",
                invoice.getId(), invoice.getStatus(), message.eventType());

        invoiceEventRepository.save(InvoiceEvent.builder()
                .eventId(message.eventId())
                .invoice(invoice)
                .eventType(message.eventType())
                .payloadJson(rawPayload)
                .kafkaOffset(offset)
                .build());
    }
}
