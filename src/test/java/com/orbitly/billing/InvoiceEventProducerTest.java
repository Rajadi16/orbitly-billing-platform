package com.orbitly.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.Invoice;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
        "orbitly.stripe.api-key=sk_test_dummy",
        "orbitly.stripe.webhook-secret=whsec_dummy",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(
        partitions = 3,
        topics     = {"invoice-events", "billing-events"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@Testcontainers
@DirtiesContext
class InvoiceEventProducerTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired InvoiceEventProducer invoiceEventProducer;
    @Autowired UserRepository       userRepository;
    @Autowired InvoiceRepository    invoiceRepository;
    @Autowired ObjectMapper         objectMapper;

    private User   testUser;
    private Invoice testInvoice;

    @BeforeEach
    void setUp() {
        testUser = userRepository.save(User.builder()
                .email("producer-test@orbitly.com")
                .password("hashed")
                .role(Role.USER)
                .build());

        testInvoice = invoiceRepository.save(Invoice.builder()
                .user(testUser)
                .amountCents(9900L)
                .currency("USD")
                .status(InvoiceStatus.PENDING)
                .build());
    }

    @AfterEach
    void tearDown() {
        invoiceRepository.deleteAll();
        userRepository.deleteAll();
    }

    @Test
    @DisplayName("publishInvoiceCreated → message on invoice-events with correct payload")
    void publish_sendsMessageWithCorrectPayload() throws Exception {
        invoiceEventProducer.publishInvoiceCreated(
                testInvoice.getId(), 9900L, "USD");

        // Give Kafka time to deliver
        Thread.sleep(2000);

        // Manual consumer to verify
        try (KafkaConsumer<String, String> consumer = buildConsumer()) {
            consumer.subscribe(List.of("invoice-events"));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();

            long deadline = System.currentTimeMillis() + 5_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }

            assertThat(records).isNotEmpty();

            ConsumerRecord<String, String> rec = records.get(0);
            assertThat(rec.key()).isEqualTo(testInvoice.getId().toString());

            InvoiceEventMessage msg = objectMapper.readValue(rec.value(), InvoiceEventMessage.class);
            assertThat(msg.eventType()).isEqualTo("invoice.created");
            assertThat(msg.invoiceId()).isEqualTo(testInvoice.getId().toString());
            assertThat(msg.amountCents()).isEqualTo(9900L);
            assertThat(msg.currency()).isEqualTo("USD");
            assertThat(msg.eventId()).isNotBlank();
        }
    }

    private KafkaConsumer<String, String> buildConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"),
                ConsumerConfig.GROUP_ID_CONFIG,          "test-group-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
    }
}
