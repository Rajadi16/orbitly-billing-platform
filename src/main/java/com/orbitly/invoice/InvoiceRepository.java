package com.orbitly.invoice;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InvoiceRepository extends JpaRepository<Invoice, UUID> {

    List<Invoice> findByUserId(UUID userId);

    List<Invoice> findByUserIdAndStatus(UUID userId, InvoiceStatus status);

    Optional<Invoice> findByStripePaymentIntentId(String stripePaymentIntentId);
}
