package com.orbitly.invoice.dto;

import com.orbitly.invoice.Invoice;

import java.time.OffsetDateTime;
import java.util.UUID;

public record InvoiceResponse(
        UUID            id,
        UUID            userId,
        long            amountCents,
        String          currency,
        String          status,
        String          stripePaymentIntentId,
        OffsetDateTime  createdAt,
        OffsetDateTime  updatedAt
) {
    public static InvoiceResponse from(Invoice invoice) {
        return new InvoiceResponse(
                invoice.getId(),
                invoice.getUser().getId(),
                invoice.getAmountCents(),
                invoice.getCurrency(),
                invoice.getStatus().name(),
                invoice.getStripePaymentIntentId(),
                invoice.getCreatedAt(),
                invoice.getUpdatedAt()
        );
    }
}
