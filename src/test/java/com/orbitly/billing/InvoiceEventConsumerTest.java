package com.orbitly.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.Invoice;
import com.orbitly.invoice.InvoiceEvent;
import com.orbitly.invoice.InvoiceEventRepository;
import com.orbitly.invoice.InvoiceRepository;
import com.orbitly.invoice.InvoiceStatus;
import com.orbitly.user.Role;
import com.orbitly.user.User;
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

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InvoiceEventConsumerTest {

    @Mock private InvoiceRepository      invoiceRepository;
    @Mock private InvoiceEventRepository invoiceEventRepository;
    @Mock private Acknowledgment         ack;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    @InjectMocks
    private InvoiceEventConsumer consumer;

    private Invoice invoice;
    private String  eventId;
    private String  payload;

    @BeforeEach
    void setUp() throws Exception {
        eventId = UUID.randomUUID().toString();
        User user = User.builder().id(UUID.randomUUID()).email("t@t.com")
                .password("x").role(Role.USER).build();
        invoice = Invoice.builder()
                .id(UUID.randomUUID()).user(user)
                .amountCents(5000L).currency("USD")
                .status(InvoiceStatus.PENDING).build();

        InvoiceEventMessage msg = new InvoiceEventMessage(
                eventId, invoice.getId().toString(),
                "invoice.created", 5000L, "USD", OffsetDateTime.now());
        payload = new ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .writeValueAsString(msg);
    }

    // ── 1. Normal processing ─────────────────────────────────────────────────

    @Test
    @DisplayName("New eventId → invoice event saved, ack sent")
    void newEvent_isProcessed() {
        when(invoiceEventRepository.existsByEventId(eventId)).thenReturn(false);
        when(invoiceRepository.findById(invoice.getId())).thenReturn(Optional.of(invoice));

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("invoice-events", 0, 10L, invoice.getId().toString(), payload);

        consumer.consume(record, ack);

        ArgumentCaptor<InvoiceEvent> captor = ArgumentCaptor.forClass(InvoiceEvent.class);
        verify(invoiceEventRepository).save(captor.capture());
        assertThat(captor.getValue().getEventId()).isEqualTo(eventId);
        assertThat(captor.getValue().getKafkaOffset()).isEqualTo(10L);
        verify(ack).acknowledge();
    }

    // ── 2. Duplicate eventId → skip ──────────────────────────────────────────

    @Test
    @DisplayName("Duplicate eventId → skipped, no invoice event saved")
    void duplicateEvent_isSkipped() {
        when(invoiceEventRepository.existsByEventId(eventId)).thenReturn(true);

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("invoice-events", 0, 11L, "key", payload);

        consumer.consume(record, ack);

        verify(invoiceEventRepository, never()).save(any());
        verify(invoiceRepository, never()).findById(any());
        verify(ack).acknowledge();
    }

    // ── 3. Invoice not found → throws (DefaultErrorHandler retries → DLQ) ───

    @Test
    @DisplayName("Invoice not found → exception thrown (triggers retry/DLQ path)")
    void invoiceNotFound_throwsException() {
        when(invoiceEventRepository.existsByEventId(eventId)).thenReturn(false);
        when(invoiceRepository.findById(any())).thenReturn(Optional.empty());

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("invoice-events", 0, 12L, "key", payload);

        org.junit.jupiter.api.Assertions.assertThrows(
                Exception.class, () -> consumer.consume(record, ack));

        verify(invoiceEventRepository, never()).save(any());
    }

    // ── 4. Malformed JSON → ack without processing ────────────────────────────

    @Test
    @DisplayName("Malformed JSON payload → logged and acked, no crash")
    void malformedPayload_acksWithoutCrash() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("invoice-events", 0, 13L, "key", "not-json");

        consumer.consume(record, ack);

        verify(invoiceEventRepository, never()).save(any());
        verify(ack).acknowledge();
    }
}
