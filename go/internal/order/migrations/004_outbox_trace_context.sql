-- W3C trace context captured when the event was written, so the trace continues
-- through the relay and broker into consumers.
ALTER TABLE outbox ADD COLUMN trace_headers jsonb NOT NULL DEFAULT '{}';
