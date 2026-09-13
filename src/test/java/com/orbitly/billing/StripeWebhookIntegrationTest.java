package com.orbitly.billing;

import com.orbitly.invoice.Invoice;
import com.orbitly.invoice.InvoiceRepository;
import com.orbitly.invoice.InvoiceStatus;
import com.orbitly.user.Role;
import com.orbitly.user.User;
import com.orbitly.user.UserRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end integration test for Stripe webhook payment_intent events.
 * Verifies that webhook signature validation, payment routing, and invoice
 * status updates work correctly.
 *
 * Test scenarios:
 *  1. payment_intent.succeeded → invoice status becomes PAID
 *  2. payment_intent.payment_failed → invoice status becomes FAILED
 *  3. Invalid signature → 400 Bad Request, no status change
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
                "orbitly.stripe.api-key=sk_test_dummy",
                "orbitly.stripe.webhook-secret=whsec_integration_test_secret_key"
        }
)
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext
class StripeWebhookIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired MockMvc mockMvc;
    @Autowired InvoiceRepository invoiceRepository;
    @Autowired UserRepository    userRepository;

    @MockBean BillingEventProducer billingEventProducer;
    @MockBean InvoiceEventProducer invoiceEventProducer;

    private static final String TEST_WEBHOOK_SECRET = "whsec_integration_test_secret_key";
    private static final String WEBHOOK_URL = "/api/v1/webhooks/stripe";

    private User    testUser;
    private Invoice testInvoice;

    @BeforeEach
    void setUp() {
        testUser = userRepository.save(User.builder()
                .email("stripe-test@orbitly.com")
                .password("hashed")
                .role(Role.USER)
                .build());

        testInvoice = invoiceRepository.save(Invoice.builder()
                .user(testUser)
                .amountCents(5000L)
                .currency("USD")
                .status(InvoiceStatus.PENDING)
                .stripePaymentIntentId("pi_test_abc123")
                .build());
    }

    @AfterEach
    void tearDown() {
        invoiceRepository.deleteAll();
        userRepository.deleteAll();
    }

    // ── 1. payment_intent.succeeded → PAID ───────────────────────────────────

    @Test
    @DisplayName("Webhook with payment_intent.succeeded → invoice status becomes PAID")
    void paymentSucceeded_updatesInvoiceToPaid() throws Exception {
        String payload = """
                {
                  "id": "evt_success_001",
                  "type": "payment_intent.succeeded",
                  "data": {
                    "object": {
                      "id": "pi_test_abc123",
                      "amount": 5000,
                      "currency": "usd",
                      "status": "succeeded"
                    }
                  }
                }
                """;

        String signature = buildStripeSignature(payload, TEST_WEBHOOK_SECRET);

        mockMvc.perform(post(WEBHOOK_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", signature)
                        .content(payload))
                .andExpect(status().isOk());

        // Verify invoice status updated to PAID
        await().atMost(java.time.Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    Invoice updated = invoiceRepository.findById(testInvoice.getId()).orElseThrow();
                    assertThat(updated.getStatus()).isEqualTo(InvoiceStatus.PAID);
                });
    }

    // ── 2. payment_intent.payment_failed → FAILED ────────────────────────────

    @Test
    @DisplayName("Webhook with payment_intent.payment_failed → invoice status becomes FAILED")
    void paymentFailed_updatesInvoiceToFailed() throws Exception {
        String payload = """
                {
                  "id": "evt_failed_001",
                  "type": "payment_intent.payment_failed",
                  "data": {
                    "object": {
                      "id": "pi_test_abc123",
                      "amount": 5000,
                      "currency": "usd",
                      "status": "failed",
                      "last_payment_error": {
                        "message": "Your card was declined"
                      }
                    }
                  }
                }
                """;

        String signature = buildStripeSignature(payload, TEST_WEBHOOK_SECRET);

        mockMvc.perform(post(WEBHOOK_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", signature)
                        .content(payload))
                .andExpect(status().isOk());

        // Verify invoice status updated to FAILED
        await().atMost(java.time.Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    Invoice updated = invoiceRepository.findById(testInvoice.getId()).orElseThrow();
                    assertThat(updated.getStatus()).isEqualTo(InvoiceStatus.FAILED);
                });
    }

    // ── 3. Invalid signature → 400, no status change ─────────────────────────

    @Test
    @DisplayName("Webhook with invalid signature → 400 Bad Request, invoice unchanged")
    void invalidSignature_returns400_invoiceUnchanged() throws Exception {
        String payload = """
                {
                  "id": "evt_invalid_001",
                  "type": "payment_intent.succeeded",
                  "data": {
                    "object": {
                      "id": "pi_test_abc123",
                      "amount": 5000,
                      "currency": "usd",
                      "status": "succeeded"
                    }
                  }
                }
                """;

        // Use wrong signature
        String invalidSignature = "t=123456789,v1=invalidhexstring";

        mockMvc.perform(post(WEBHOOK_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", invalidSignature)
                        .content(payload))
                .andExpect(status().isBadRequest());

        // Verify invoice status remains PENDING
        Invoice unchanged = invoiceRepository.findById(testInvoice.getId()).orElseThrow();
        assertThat(unchanged.getStatus()).isEqualTo(InvoiceStatus.PENDING);
    }

    // ── 4. Unknown payment_intent → logs warning, no crash ───────────────────

    @Test
    @DisplayName("Webhook for non-existent payment_intent → 200 OK but no status change")
    void unknownPaymentIntent_returns200_noStatusChange() throws Exception {
        String payload = """
                {
                  "id": "evt_unknown_001",
                  "type": "payment_intent.succeeded",
                  "data": {
                    "object": {
                      "id": "pi_nonexistent_xyz",
                      "amount": 9999,
                      "currency": "usd",
                      "status": "succeeded"
                    }
                  }
                }
                """;

        String signature = buildStripeSignature(payload, TEST_WEBHOOK_SECRET);

        mockMvc.perform(post(WEBHOOK_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", signature)
                        .content(payload))
                .andExpect(status().isOk());

        // Verify test invoice remains PENDING (unchanged)
        Invoice unchanged = invoiceRepository.findById(testInvoice.getId()).orElseThrow();
        assertThat(unchanged.getStatus()).isEqualTo(InvoiceStatus.PENDING);
    }

    // ── 5. Missing Stripe-Signature header → 400 ─────────────────────────────

    @Test
    @DisplayName("Webhook without Stripe-Signature header → 400 Bad Request")
    void missingSignatureHeader_returns400() throws Exception {
        String payload = """
                {
                  "id": "evt_no_sig_001",
                  "type": "payment_intent.succeeded",
                  "data": {
                    "object": {
                      "id": "pi_test_abc123"
                    }
                  }
                }
                """;

        mockMvc.perform(post(WEBHOOK_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Replicates Stripe's HMAC-SHA256 signature scheme.
     * Format: "t={timestamp},v1={hex(HMAC-SHA256(secret, timestamp.payload))}"
     */
    private static String buildStripeSignature(String payload, String secret) throws Exception {
        long timestamp = System.currentTimeMillis() / 1000;
        String signed  = timestamp + "." + payload;

        // Strip "whsec_" prefix — Stripe stores raw key after the prefix
        String rawSecret = secret.startsWith("whsec_") ? secret.substring(6) : secret;

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(rawSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hmac = mac.doFinal(signed.getBytes(StandardCharsets.UTF_8));

        StringBuilder hex = new StringBuilder();
        for (byte b : hmac) hex.append(String.format("%02x", b));

        return "t=" + timestamp + ",v1=" + hex;
    }
}
