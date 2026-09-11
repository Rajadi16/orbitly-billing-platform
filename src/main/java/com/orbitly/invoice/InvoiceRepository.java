package com.orbitly.invoice;

import org.springframework.data.jpa.repository.JpaRepository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InvoiceRepository extends JpaRepository<Invoice, UUID> {

    // Traverses Invoice.user.id — note underscore for explicit path
    List<Invoice> findByUser_Id(UUID userId);

    Page<Invoice> findByUser_Id(UUID userId, Pageable pageable);

    List<Invoice> findByUser_IdAndStatus(UUID userId, InvoiceStatus status);

    Optional<Invoice> findByStripePaymentIntentId(String stripePaymentIntentId);
}
