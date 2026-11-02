-- ISO 4217 currency of unit_price_cents. Existing products were priced in US dollars.
ALTER TABLE products
    ADD COLUMN currency_code text NOT NULL DEFAULT 'USD' CHECK (currency_code ~ '^[A-Z]{3}$');
