package com.orbitly.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BillingEventConsumerTest {

    @Mock private InvoiceRepository      invoiceRepository;
    @Mock private InvoiceEventRepository invoiceEventRepository;
    @Mock private Acknowledgment         ack;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private BillingEventConsumer consumer;

    private Invoice invoice;

    @BeforeEach
    void setUp() {
        invoice = Invoice.builder()
                .id(UUID.randomUUID())
                .amountCents(5000L)
                .currency("USD")
                .status(InvoiceStatus.PENDING)
                .stripePaymentIntentId("pi_test_123")
                .build();
    }

    // ── 1. Duplicate offset → skip ───────────────────────────────────────────

    @Test
    @DisplayName("Duplicate Kafka offset → skipped, invoice NOT updated")
    void duplicateOffset_isSkipped() {
        when(invoiceEventRepository.existsByKafkaOffset(42L)).thenReturn(true);

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("billing-events", 0, 42L, "key", "{}");

        consumer.consume(record, ack);

        verify(invoiceRepository, never()).save(any());
        verify(ack).acknowledge();
    }

    // ── 2. payment_succeeded → PAID ──────────────────────────────────────────

    @Test
    @DisplayName("invoice.payment_succeeded → invoice status set to PAID")
    void paymentSucceeded_setsStatusPaid() throws Exception {
        when(invoiceEventRepository.existsByKafkaOffset(anyLong())).thenReturn(false);
        when(invoiceRepository.findByStripePaymentIntentId("pi_test_123"))
                .thenReturn(Optional.of(invoice));

        String payload = """
                {
                  "id": "evt_001",
                  "type": "invoice.payment_succeeded",
                  "data": { "object": { "payment_intent": "pi_test_123" } }
                }
                """;

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("billing-events", 0, 1L, "evt_001", payload);

        consumer.consume(record, ack);

        ArgumentCaptor<Invoice> captor = ArgumentCaptor.forClass(Invoice.class);
        verify(invoiceRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(InvoiceStatus.PAID);

        verify(invoiceEventRepository).save(any(InvoiceEvent.class));
        verify(ack).acknowledge();
    }

    // ── 3. payment_failed → FAILED ───────────────────────────────────────────

    @Test
    @DisplayName("invoice.payment_failed → invoice status set to FAILED")
    void paymentFailed_setsStatusFailed() {
        when(invoiceEventRepository.existsByKafkaOffset(anyLong())).thenReturn(false);
        when(invoiceRepository.findByStripePaymentIntentId("pi_test_123"))
                .thenReturn(Optional.of(invoice));

        String payload = """
                {
                  "id": "evt_002",
                  "type": "invoice.payment_failed",
                  "data": { "object": { "payment_intent": "pi_test_123" } }
                }
                """;

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("billing-events", 0, 2L, "evt_002", payload);

        consumer.consume(record, ack);

        ArgumentCaptor<Invoice> captor = ArgumentCaptor.forClass(Invoice.class);
        verify(invoiceRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(InvoiceStatus.FAILED);
        verify(ack).acknowledge();
    }

    // ── 4. Unknown event type → no invoice update, still acks ───────────────

    @Test
    @DisplayName("Unknown event type → no invoice update, ack still sent")
    void unknownEventType_noInvoiceUpdate() {
        when(invoiceEventRepository.existsByKafkaOffset(anyLong())).thenReturn(false);

        String payload = """
                {
                  "id": "evt_003",
                  "type": "charge.refunded",
                  "data": { "object": {} }
                }
                """;

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("billing-events", 0, 3L, "evt_003", payload);

        consumer.consume(record, ack);

        verify(invoiceRepository, never()).save(any());
        verify(ack).acknowledge();
    }
}
