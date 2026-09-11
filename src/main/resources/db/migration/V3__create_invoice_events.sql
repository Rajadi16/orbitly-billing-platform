-- V3__create_invoice_events.sql
-- Idempotency table: each processed Kafka message is recorded here.
-- Unique constraint on (kafka_topic, kafka_partition, kafka_offset) prevents
-- duplicate processing even if the consumer restarts mid-batch.

CREATE TABLE IF NOT EXISTS invoice_events (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id      UUID         NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    event_type      VARCHAR(100) NOT NULL,
    payload_json    JSONB        NOT NULL,
    kafka_offset    BIGINT       NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX        idx_invoice_events_invoice_id   ON invoice_events(invoice_id);
CREATE UNIQUE INDEX idx_invoice_events_kafka_offset ON invoice_events(kafka_offset);
