-- V4__add_event_id_to_invoice_events.sql
-- Adds the eventId field used by the InvoiceEventConsumer for idempotency.
-- The BillingEventConsumer uses kafkaOffset; the InvoiceEventConsumer uses eventId
-- from the InvoiceEventMessage payload so it survives partition reassignments.

ALTER TABLE invoice_events
    ADD COLUMN event_id VARCHAR(255);

CREATE UNIQUE INDEX idx_invoice_events_event_id ON invoice_events(event_id)
    WHERE event_id IS NOT NULL;
