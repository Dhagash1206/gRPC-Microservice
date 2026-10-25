-- 'cancelled' rows are fences written by CancelCharge before any charge exists,
-- so they carry no customer or amount.
ALTER TABLE payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payments DROP CONSTRAINT payments_amount_cents_check;
ALTER TABLE payments ALTER COLUMN customer_id DROP NOT NULL;
ALTER TABLE payments ALTER COLUMN amount_cents DROP NOT NULL;
ALTER TABLE payments ADD COLUMN refunded_at timestamptz;

ALTER TABLE payments ADD CONSTRAINT payments_status_check
    CHECK (status IN ('succeeded', 'declined', 'cancelled', 'refunded'));
ALTER TABLE payments ADD CONSTRAINT payments_charge_fields_check
    CHECK (status = 'cancelled' OR (customer_id IS NOT NULL AND amount_cents > 0));
