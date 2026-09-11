package com.orbitly.invoice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateInvoiceRequest(

        @Positive(message = "amountCents must be positive")
        long amountCents,

        @NotBlank(message = "currency is required")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO code (e.g. USD)")
        String currency
) {}
