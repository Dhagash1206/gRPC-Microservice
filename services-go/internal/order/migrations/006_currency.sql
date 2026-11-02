-- ISO 4217 currency of every *_cents amount on the order. Existing orders were in US dollars.
ALTER TABLE orders
    ADD COLUMN currency_code text NOT NULL DEFAULT 'USD' CHECK (currency_code ~ '^[A-Z]{3}$');
ALTER TABLE orders ALTER COLUMN currency_code DROP DEFAULT;
