package com.orbitly.billing;

import java.time.OffsetDateTime;

/**
 * Payload published to the invoice-events Kafka topic.
 * Keyed by invoiceId so all events for one invoice land on the same partition.
 */
public record InvoiceEventMessage(
        String         eventId,      // random UUID — idempotency key for consumer
        String         invoiceId,
        String         eventType,    // e.g. "invoice.created"
        long           amountCents,
        String         currency,
        OffsetDateTime timestamp
) {}
