package com.orbitly.billing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(
        controllers = StripeWebhookController.class,
        excludeAutoConfiguration = {
                org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class,
                org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration.class,
                org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration.class
        },
        properties  = {
                "orbitly.stripe.webhook-secret=whsec_test_secret_key_for_unit_tests",
                "orbitly.stripe.api-key=sk_test_dummy",
                "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
                "orbitly.jwt.expiration-ms=3600000"
        }
)
class StripeWebhookControllerTest {

    @Autowired MockMvc mockMvc;

    @MockBean BillingEventProducer billingEventProducer;
    @MockBean StripeWebhookService  stripeWebhookService;

    // The test webhook secret baked into properties below
    private static final String TEST_SECRET = "whsec_test_secret_key_for_unit_tests";

    private static final String PAYLOAD = """
            {
              "id": "evt_test_001",
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

    // ── 1. Valid signature → 200 ─────────────────────────────────────────────

    @Test
    @DisplayName("Valid Stripe-Signature → 200 OK")
    void validSignature_returns200() throws Exception {
        String sigHeader = buildStripeSignature(PAYLOAD, TEST_SECRET);

        mockMvc.perform(post("/api/v1/webhooks/stripe")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", sigHeader)
                        .content(PAYLOAD))
                .andExpect(status().isOk());
    }

    // ── 2. Invalid signature → 400 ───────────────────────────────────────────

    @Test
    @DisplayName("Invalid Stripe-Signature → 400 Bad Request")
    void invalidSignature_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/webhooks/stripe")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", "t=123,v1=invalidsignature")
                        .content(PAYLOAD))
                .andExpect(status().isBadRequest());
    }

    // ── 3. Missing signature header → 400 ────────────────────────────────────

    @Test
    @DisplayName("Missing Stripe-Signature header → 400 Bad Request")
    void missingSignature_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/webhooks/stripe")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(PAYLOAD))
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
