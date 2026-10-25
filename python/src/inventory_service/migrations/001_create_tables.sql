CREATE TABLE products (
    sku                text PRIMARY KEY,
    name               text NOT NULL,
    unit_price_cents   bigint NOT NULL CHECK (unit_price_cents > 0),
    available_quantity integer NOT NULL CHECK (available_quantity >= 0)
);

CREATE TABLE reservations (
    id          text PRIMARY KEY,
    order_id    text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    released_at timestamptz
);

CREATE TABLE reservation_items (
    reservation_id text NOT NULL REFERENCES reservations (id),
    sku            text NOT NULL REFERENCES products (sku),
    quantity       integer NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, sku)
);
