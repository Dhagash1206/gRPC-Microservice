-- ISO 4217 currency of amount_cents. Fences carry no amount, so no currency either.
ALTER TABLE payments ADD COLUMN currency_code text;
UPDATE payments SET currency_code = 'USD' WHERE status <> 'cancelled';
ALTER TABLE payments ADD CONSTRAINT payments_currency_check
    CHECK (status = 'cancelled' OR currency_code ~ '^[A-Z]{3}$');
