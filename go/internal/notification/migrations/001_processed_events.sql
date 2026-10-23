-- Durable deduplication shared by every notification replica.
CREATE TABLE processed_events (
    event_id     uuid PRIMARY KEY,
    routing_key  text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
