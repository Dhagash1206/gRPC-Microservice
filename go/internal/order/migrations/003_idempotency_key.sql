-- Client-supplied idempotency keys are scoped per customer. request_hash detects a
-- key reused with a different request.
ALTER TABLE orders
    ADD COLUMN idempotency_key text,
    ADD COLUMN request_hash text;

CREATE UNIQUE INDEX orders_customer_idempotency_key_idx
    ON orders (customer_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
