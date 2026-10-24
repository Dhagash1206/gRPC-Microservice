-- held -> committed (order confirmed) | released (compensation, cancellation, expiry).
ALTER TABLE reservations
    ADD COLUMN status text NOT NULL DEFAULT 'held' CHECK (status IN ('held', 'committed', 'released')),
    ADD COLUMN expires_at timestamptz;

UPDATE reservations SET status = 'released' WHERE released_at IS NOT NULL;
UPDATE reservations SET expires_at = created_at + interval '15 minutes';
ALTER TABLE reservations ALTER COLUMN expires_at SET NOT NULL;

CREATE INDEX reservations_held_expiry_idx ON reservations (expires_at) WHERE status = 'held';
