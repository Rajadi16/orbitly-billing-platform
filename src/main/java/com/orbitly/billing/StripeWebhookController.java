package com.orbitly.billing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stripe.exception.SignatureVerificationException;
import com.stripe.model.Event;
import com.stripe.net.Webhook;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/webhooks")
@RequiredArgsConstructor
public class StripeWebhookController {

    private final BillingEventProducer  billingEventProducer;
    private final StripeWebhookService  stripeWebhookService;
    private final ObjectMapper          objectMapper;

    @Value("${orbitly.stripe.webhook-secret}")
    private String webhookSecret;

    /**
     * Stripe calls this endpoint for every event (test or live mode).
     * Must respond with 2xx within 30 s or Stripe retries.
     *
     * Flow:
     *  1. Verify Stripe-Signature header — reject 400 on mismatch (no detail leaked)
     *  2. Route payment_intent events to StripeWebhookService (updates invoice status)
     *  3. Publish raw payload to billing-events Kafka topic for audit trail
     *  4. Return 200 immediately
     *
     * Raw String body is required for signature verification —
     * Spring must NOT parse it to a POJO before this point.
     */
    @PostMapping(value = "/stripe", consumes = "application/json")
    public ResponseEntity<String> handleStripeEvent(
            @RequestBody String rawPayload,
            @RequestHeader("Stripe-Signature") String stripeSignature) {

        Event event;
        try {
            event = Webhook.constructEvent(rawPayload, stripeSignature, webhookSecret);
        } catch (SignatureVerificationException e) {
            log.warn("Stripe webhook signature verification failed — possible spoofed request");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid signature");
        }

        log.info("Stripe webhook received [id={}, type={}]", event.getId(), event.getType());

        // Async audit trail — raw payload → Kafka billing-events
        billingEventProducer.publish(event.getId(), rawPayload);

        // Synchronous status update for payment intents
        handlePaymentEvent(event.getType(), rawPayload);

        return ResponseEntity.ok("received");
    }

    // ── Payment intent routing ────────────────────────────────────────────────

    private void handlePaymentEvent(String eventType, String rawPayload) {
        String piId = extractPaymentIntentId(rawPayload);
        if (piId == null) return;

        switch (eventType) {
            case "payment_intent.succeeded" ->
                    stripeWebhookService.handlePaymentIntentSucceeded(piId, rawPayload);
            case "payment_intent.payment_failed" ->
                    stripeWebhookService.handlePaymentIntentFailed(piId, rawPayload);
            default ->
                    log.debug("Unhandled Stripe event type '{}' — no invoice action", eventType);
        }
    }

    private String extractPaymentIntentId(String rawPayload) {
        try {
            JsonNode root = objectMapper.readTree(rawPayload);
            JsonNode obj  = root.path("data").path("object");
            // payment_intent events: data.object.id = pi_xxx
            if (obj.has("id") && obj.path("id").asText("").startsWith("pi_")) {
                return obj.path("id").asText();
            }
        } catch (Exception e) {
            log.warn("Could not extract paymentIntentId from payload: {}", e.getMessage());
        }
        return null;
    }
}
