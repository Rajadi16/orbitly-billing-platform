-- V2__create_invoices.sql

CREATE TABLE IF NOT EXISTS invoices (
    id                        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                   UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount_cents              BIGINT       NOT NULL,
    currency                  VARCHAR(3)   NOT NULL DEFAULT 'USD',
    status                    VARCHAR(20)  NOT NULL DEFAULT 'DRAFT',
    stripe_payment_intent_id  VARCHAR(255),
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_invoice_status CHECK (status IN ('DRAFT','PENDING','PAID','FAILED')),
    CONSTRAINT chk_amount_positive CHECK (amount_cents > 0)
);

CREATE INDEX idx_invoices_user_id ON invoices(user_id);
CREATE INDEX idx_invoices_status  ON invoices(status);
