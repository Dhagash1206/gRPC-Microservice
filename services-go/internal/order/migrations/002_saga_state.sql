-- Persisted saga state. Transitions:
--   pending -> confirmed | failed | compensating -> failed
--   confirmed -> cancelling -> cancelled
ALTER TABLE orders
    ADD COLUMN status text NOT NULL DEFAULT 'confirmed'
        CHECK (status IN ('pending', 'confirmed', 'compensating', 'failed', 'cancelling', 'cancelled')),
    ADD COLUMN failure_reason text NOT NULL DEFAULT '',
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE orders ALTER COLUMN status DROP DEFAULT;
ALTER TABLE orders ALTER COLUMN reservation_id SET DEFAULT '';
ALTER TABLE orders ALTER COLUMN payment_id SET DEFAULT '';

-- Keyset pagination for ListOrders.
DROP INDEX orders_customer_id_created_at_idx;
CREATE INDEX orders_customer_page_idx ON orders (customer_id, created_at DESC, id DESC);

-- Recovery worker scan.
CREATE INDEX orders_in_flight_idx ON orders (updated_at)
    WHERE status IN ('pending', 'compensating', 'cancelling');
