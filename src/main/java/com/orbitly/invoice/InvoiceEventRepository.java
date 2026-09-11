package com.orbitly.invoice;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface InvoiceEventRepository extends JpaRepository<InvoiceEvent, UUID> {

    /** Used by the idempotent consumer to check if a Kafka offset was already processed. */
    boolean existsByKafkaOffset(long kafkaOffset);

    Optional<InvoiceEvent> findByKafkaOffset(long kafkaOffset);
}
