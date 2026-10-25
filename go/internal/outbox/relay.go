// Package outbox implements the transactional outbox relay: events are written
// in the same database transaction as the order, then published asynchronously.
// Delivery is at-least-once; consumers deduplicate by event ID.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer          = otel.Tracer("grpcshop/outbox")
	meter           = otel.Meter("grpcshop/outbox")
	publishedEvents metric.Int64Counter
)

func init() {
	publishedEvents, _ = meter.Int64Counter("grpcshop.outbox.published", metric.WithDescription("Events confirmed by the broker"))
}

// RegisterBacklogMetrics reports how many events await publishing and how old the
// oldest is: a growing age means the broker is unreachable or the relay is stuck.
func RegisterBacklogMetrics(pool *pgxpool.Pool) error {
	backlog, err := meter.Int64ObservableGauge("grpcshop.outbox.backlog", metric.WithDescription("Unpublished events"))
	if err != nil {
		return err
	}
	oldestAge, err := meter.Float64ObservableGauge("grpcshop.outbox.oldest_age", metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest unpublished event"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		var count int64
		var ageSeconds float64
		err := pool.QueryRow(ctx, `SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)
			FROM outbox WHERE published_at IS NULL`).Scan(&count, &ageSeconds)
		if err != nil {
			return err
		}
		observer.ObserveInt64(backlog, count)
		observer.ObserveFloat64(oldestAge, ageSeconds)
		return nil
	}, backlog, oldestAge)
	return err
}

type Message struct {
	ID         string
	RoutingKey string
	Payload    []byte
	RequestID  string
	// W3C trace context (traceparent/tracestate) linking the event to its request.
	TraceHeaders map[string]string
}

type Publisher interface {
	// Publish returns the IDs of messages the broker durably accepted.
	Publish(ctx context.Context, messages []Message) (publishedIDs []string, err error)
}

type Relay struct {
	pool         *pgxpool.Pool
	publisher    Publisher
	pollInterval time.Duration
	batchSize    int
	logger       *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, publisher Publisher, logger *slog.Logger) *Relay {
	return &Relay{pool: pool, publisher: publisher, pollInterval: 500 * time.Millisecond, batchSize: 100, logger: logger}
}

const maxFailureBackoff = 10 * time.Second

func (relay *Relay) Run(ctx context.Context) {
	relay.logger.Info("outbox relay started")
	failureBackoff := relay.pollInterval
	for {
		wait := relay.pollInterval
		publishedCount, err := relay.publishBatch(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			relay.logger.Error("outbox relay batch failed", "error", err, "retry_in", failureBackoff.String())
			wait = failureBackoff
			failureBackoff = min(failureBackoff*2, maxFailureBackoff)
		case publishedCount == relay.batchSize:
			// A full batch means more rows are likely waiting, so skip the sleep.
			failureBackoff = relay.pollInterval
			continue
		default:
			failureBackoff = relay.pollInterval
		}
		select {
		case <-ctx.Done():
			relay.logger.Info("outbox relay stopped")
			return
		case <-time.After(wait):
		}
	}
}

func (relay *Relay) publishBatch(ctx context.Context) (int, error) {
	transaction, err := relay.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer transaction.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck

	// SKIP LOCKED lets several order replicas relay concurrently without double-sending a row.
	rows, err := transaction.Query(ctx, `
		SELECT id::text, routing_key, payload, request_id, trace_headers FROM outbox
		WHERE published_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`, relay.batchSize)
	if err != nil {
		return 0, err
	}
	messages, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Message, error) {
		var message Message
		err := row.Scan(&message.ID, &message.RoutingKey, &message.Payload, &message.RequestID, &message.TraceHeaders)
		return message, err
	})
	if err != nil || len(messages) == 0 {
		return 0, err
	}

	// One producer span per event, parented to the request that wrote it; its
	// context replaces the stored one so consumers become its children.
	spans := make(map[string]trace.Span, len(messages))
	for index := range messages {
		message := &messages[index]
		parentCtx := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(message.TraceHeaders))
		spanCtx, span := tracer.Start(parentCtx, "publish "+message.RoutingKey, trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("messaging.system", "rabbitmq"),
				attribute.String("messaging.destination.name", message.RoutingKey),
				attribute.String("messaging.message.id", message.ID)))
		message.TraceHeaders = map[string]string{}
		otel.GetTextMapPropagator().Inject(spanCtx, propagation.MapCarrier(message.TraceHeaders))
		spans[message.ID] = span
	}

	publishedIDs, publishErr := relay.publisher.Publish(ctx, messages)
	publishedSet := make(map[string]bool, len(publishedIDs))
	for _, messageID := range publishedIDs {
		publishedSet[messageID] = true
	}
	for messageID, span := range spans {
		if !publishedSet[messageID] {
			span.SetStatus(codes.Error, "not confirmed by broker")
		}
		span.End()
	}
	publishedEvents.Add(ctx, int64(len(publishedIDs)))
	if publishedIDs == nil {
		publishedIDs = []string{} // pgx encodes nil as NULL, which would break NOT (id = ANY(...)).
	}
	if len(publishedIDs) > 0 {
		if _, err := transaction.Exec(ctx,
			"UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])", publishedIDs); err != nil {
			return 0, err
		}
	}
	if len(publishedIDs) < len(messages) {
		if _, err := transaction.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1
			WHERE id = ANY($1::uuid[]) AND NOT (id = ANY($2::uuid[]))`, messageIDs(messages), publishedIDs); err != nil {
			return 0, err
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		return 0, err
	}
	if len(publishedIDs) > 0 {
		relay.logger.Info("published events", "count", len(publishedIDs))
	}
	return len(publishedIDs), publishErr
}

func messageIDs(messages []Message) []string {
	ids := make([]string, len(messages))
	for index, message := range messages {
		ids[index] = message.ID
	}
	return ids
}
