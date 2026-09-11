package com.orbitly.billing;

import com.orbitly.invoice.Invoice;
import com.orbitly.invoice.InvoiceRepository;
import com.orbitly.invoice.InvoiceStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Handles Stripe payment_intent events after webhook signature verification.
 * Invoice status is updated synchronously; caller (controller) returns 200 fast.
 *
 * Async Kafka path: StripeWebhookController also publishes the raw event to
 * billing-events for audit trail (see BillingEventProducer).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StripeWebhookService {

    private final InvoiceRepository      invoiceRepository;
    private final InvoiceEventProducer   invoiceEventProducer;

    @Transactional
    public void handlePaymentIntentSucceeded(String paymentIntentId, String rawPayload) {
        updateInvoiceStatus(paymentIntentId, InvoiceStatus.PAID, "payment_intent.succeeded");
        invoiceEventProducer.publishPaymentUpdated(paymentIntentId, "payment.updated.succeeded", rawPayload);
    }

    @Transactional
    public void handlePaymentIntentFailed(String paymentIntentId, String rawPayload) {
        updateInvoiceStatus(paymentIntentId, InvoiceStatus.FAILED, "payment_intent.payment_failed");
        invoiceEventProducer.publishPaymentUpdated(paymentIntentId, "payment.updated.failed", rawPayload);
    }

    private void updateInvoiceStatus(String piId, InvoiceStatus newStatus, String eventType) {
        Optional<Invoice> opt = invoiceRepository.findByStripePaymentIntentId(piId);
        if (opt.isEmpty()) {
            log.warn("No invoice found for paymentIntentId='{}' (event='{}')", piId, eventType);
            return;
        }
        Invoice invoice = opt.get();
        invoice.setStatus(newStatus);
        invoiceRepository.save(invoice);
        log.info("Invoice [id={}] → {} via Stripe event '{}'", invoice.getId(), newStatus, eventType);
    }
}
