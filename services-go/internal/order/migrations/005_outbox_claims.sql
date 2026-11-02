-- Claims let the relay commit before publishing: a claimed row is skipped by other
-- relays until claimed_until passes (e.g. the claiming process crashed).
-- Events the broker keeps rejecting are parked (parked_at) instead of retried forever.
ALTER TABLE outbox
    ADD COLUMN claimed_until timestamptz,
    ADD COLUMN parked_at timestamptz;

DROP INDEX outbox_unpublished_idx;
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL AND parked_at IS NULL;

-- Retention cleanup of published events.
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
