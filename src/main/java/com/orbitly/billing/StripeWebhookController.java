package com.orbitly.billing;

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

    private final BillingEventProducer producer;

    @Value("${orbitly.stripe.webhook-secret}")
    private String webhookSecret;

    /**
     * Stripe calls this endpoint for every event in test/live mode.
     * We MUST respond with 2xx within 30 s or Stripe will retry.
     *
     * Flow:
     *  1. Verify Stripe-Signature header (prevents spoofed payloads)
     *  2. Publish raw JSON to Kafka (async — returns immediately)
     *  3. Return 200 OK to Stripe
     *
     * The raw String body is required for signature verification —
     * do NOT let Spring parse it as a POJO before this point.
     */
    @PostMapping(value = "/stripe", consumes = "application/json")
    public ResponseEntity<String> handleStripeEvent(
            @RequestBody String rawPayload,
            @RequestHeader("Stripe-Signature") String stripeSignature) {

        Event event;
        try {
            event = Webhook.constructEvent(rawPayload, stripeSignature, webhookSecret);
        } catch (SignatureVerificationException e) {
            log.warn("Invalid Stripe signature: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Invalid Stripe signature");
        }

        log.info("Received Stripe event [id={}, type={}]", event.getId(), event.getType());
        producer.publish(event.getId(), rawPayload);

        return ResponseEntity.ok("Event received");
    }
}
