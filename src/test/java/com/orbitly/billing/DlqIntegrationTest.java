package com.orbitly.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.Invoice;
import com.orbitly.invoice.InvoiceEvent;
import com.orbitly.invoice.InvoiceEventRepository;
import com.orbitly.invoice.InvoiceRepository;
import com.orbitly.invoice.InvoiceStatus;
import com.orbitly.user.Role;
import com.orbitly.user.User;
import com.orbitly.user.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying that failed invoice-events messages are retried
 * and then routed to the dead-letter queue (invoice-events.dlq) after exhausting
 * retries.
 *
 * Test scenario:
 *  1. Publish an invoice-event message that references a non-existent invoice
 *  2. Consumer throws IllegalStateException
 *  3. DefaultErrorHandler retries 3 times with 1s backoff
 *  4. After retries exhausted, DeadLetterPublishingRecoverer sends to invoice-events.dlq
 *  5. DlqConsumer logs the failed message
 */
@SpringBootTest(properties = {
        "orbitly.security.enabled=false",
        "spring.kafka.enabled=true",
        "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
        "orbitly.jwt.expiration-ms=3600000",
        "orbitly.stripe.api-key=sk_test_dummy",
        "orbitly.stripe.webhook-secret=whsec_dummy",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(
        partitions = 1,
        topics     = {"invoice-events", "invoice-events.dlq"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@Testcontainers
@DirtiesContext
class DlqIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired InvoiceEventRepository        invoiceEventRepository;
    @Autowired InvoiceRepository             invoiceRepository;
    @Autowired UserRepository                userRepository;
    @Autowired ObjectMapper                  objectMapper;

    private User testUser;

    @BeforeEach
    void setUp() {
        testUser = userRepository.save(User.builder()
                .email("dlq-test@orbitly.com")
                .password("hashed")
                .role(Role.USER)
                .build());
    }

    @AfterEach
    void tearDown() {
        invoiceEventRepository.deleteAll();
        invoiceRepository.deleteAll();
        userRepository.deleteAll();
    }

    @Test
    @DisplayName("Failed invoice event → retries exhausted → message lands in DLQ")
    void failedProcessing_exhaustsRetries_sendsToDeadLetterQueue() throws Exception {
        // ── 1. Create a message referencing a non-existent invoice ──────────
        UUID nonExistentInvoiceId = UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();

        InvoiceEventMessage message = new InvoiceEventMessage(
                eventId,
                nonExistentInvoiceId.toString(),
                "invoice.created",
                5000L,
                "USD",
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(message);

        // ── 2. Publish to invoice-events ─────────────────────────────────────
        kafkaTemplate.send("invoice-events", nonExistentInvoiceId.toString(), payload);
        kafkaTemplate.flush();

        // ── 3. Wait for retries + DLQ routing ────────────────────────────────
        // DefaultErrorHandler: 3 retries × 1s = ~3s + processing overhead
        // Allow up to 10s for all retries and DLQ delivery
        Thread.sleep(10_000);

        // ── 4. Verify message landed in DLQ ──────────────────────────────────
        try (KafkaConsumer<String, String> dlqConsumer = buildDlqConsumer()) {
            dlqConsumer.subscribe(List.of("invoice-events.dlq"));

            List<ConsumerRecord<String, String>> dlqRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 5_000;

            while (dlqRecords.isEmpty() && System.currentTimeMillis() < deadline) {
                dlqConsumer.poll(Duration.ofMillis(500)).forEach(dlqRecords::add);
            }

            assertThat(dlqRecords)
                    .as("DLQ should contain the failed message after retries exhausted")
                    .isNotEmpty();

            ConsumerRecord<String, String> dlqRecord = dlqRecords.get(0);
            assertThat(dlqRecord.key()).isEqualTo(nonExistentInvoiceId.toString());

            InvoiceEventMessage dlqMessage = objectMapper.readValue(
                    dlqRecord.value(), InvoiceEventMessage.class);
            assertThat(dlqMessage.eventId()).isEqualTo(eventId);
            assertThat(dlqMessage.invoiceId()).isEqualTo(nonExistentInvoiceId.toString());
        }

        // ── 5. Verify no InvoiceEvent was persisted ─────────────────────────
        assertThat(invoiceEventRepository.existsByEventId(eventId))
                .as("Failed event should NOT be saved to invoice_events table")
                .isFalse();
    }

    @Test
    @DisplayName("Valid invoice event → processed successfully, NOT routed to DLQ")
    void successfulProcessing_doesNotSendToDlq() throws Exception {
        // ── 1. Create a valid invoice ────────────────────────────────────────
        Invoice invoice = invoiceRepository.save(Invoice.builder()
                .user(testUser)
                .amountCents(9900L)
                .currency("USD")
                .status(InvoiceStatus.PENDING)
                .build());

        String eventId = UUID.randomUUID().toString();
        InvoiceEventMessage message = new InvoiceEventMessage(
                eventId,
                invoice.getId().toString(),
                "invoice.created",
                9900L,
                "USD",
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(message);

        // ── 2. Publish to invoice-events ─────────────────────────────────────
        kafkaTemplate.send("invoice-events", invoice.getId().toString(), payload);
        kafkaTemplate.flush();

        // ── 3. Wait for processing ───────────────────────────────────────────
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() ->
                        assertThat(invoiceEventRepository.existsByEventId(eventId))
                                .isTrue());

        // ── 4. Verify DLQ is empty ───────────────────────────────────────────
        try (KafkaConsumer<String, String> dlqConsumer = buildDlqConsumer()) {
            dlqConsumer.subscribe(List.of("invoice-events.dlq"));

            List<ConsumerRecord<String, String>> dlqRecords = new ArrayList<>();
            dlqConsumer.poll(Duration.ofMillis(2_000)).forEach(dlqRecords::add);

            assertThat(dlqRecords)
                    .as("DLQ should be empty for successful processing")
                    .isEmpty();
        }

        // ── 5. Verify InvoiceEvent was persisted ────────────────────────────
        assertThat(invoiceEventRepository.existsByEventId(eventId)).isTrue();
    }

    private KafkaConsumer<String, String> buildDlqConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"),
                ConsumerConfig.GROUP_ID_CONFIG,          "dlq-test-group-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
    }
}
