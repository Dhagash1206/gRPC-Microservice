CREATE TABLE payments (
    id              text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    customer_id     text NOT NULL,
    amount_cents    bigint NOT NULL CHECK (amount_cents > 0),
    status          text NOT NULL CHECK (status IN ('succeeded', 'declined')),
    decline_reason  text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
