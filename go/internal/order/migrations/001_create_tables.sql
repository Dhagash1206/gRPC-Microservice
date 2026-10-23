CREATE TABLE orders (
    id             uuid PRIMARY KEY,
    customer_id    text NOT NULL,
    total_cents    bigint NOT NULL CHECK (total_cents > 0),
    reservation_id text NOT NULL,
    payment_id     text NOT NULL,
    created_at     timestamptz NOT NULL
);

CREATE INDEX orders_customer_id_created_at_idx ON orders (customer_id, created_at DESC);

CREATE TABLE order_lines (
    order_id         uuid NOT NULL REFERENCES orders (id),
    line_number      integer NOT NULL,
    sku              text NOT NULL,
    product_name     text NOT NULL,
    quantity         integer NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents > 0),
    PRIMARY KEY (order_id, line_number)
);

-- Transactional outbox: written atomically with the order, drained by the relay.
CREATE TABLE outbox (
    id           uuid PRIMARY KEY,
    routing_key  text NOT NULL,
    payload      bytea NOT NULL,
    request_id   text NOT NULL DEFAULT '',
    attempts     integer NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
