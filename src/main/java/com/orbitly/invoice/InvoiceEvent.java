package com.orbitly.invoice;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Idempotency log for Kafka consumer.
 * Before processing any billing event, the consumer checks whether a row
 * with the same kafkaOffset already exists.  If it does, the message is
 * skipped — guaranteeing at-most-once processing even after consumer restart.
 */
@Entity
@Table(name = "invoice_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "invoice_id", nullable = false)
    private Invoice invoice;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    /** Raw Stripe / internal event payload stored as JSONB */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload_json", nullable = false, columnDefinition = "jsonb")
    private String payloadJson;

    /** Kafka record offset — unique constraint in DB prevents duplicate processing */
    @Column(name = "kafka_offset", nullable = false, unique = true)
    private long kafkaOffset;

    @Column(name = "processed_at", nullable = false, updatable = false)
    @Builder.Default
    private OffsetDateTime processedAt = OffsetDateTime.now();
}
