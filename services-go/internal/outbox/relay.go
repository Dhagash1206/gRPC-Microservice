// Package outbox implements the transactional outbox relay: events are written
// in the same database transaction as the order, then published asynchronously.
// Delivery is at-least-once; consumers deduplicate by event ID.
package outbox

import (
	"context"
	"log/slog"
	"sort"
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
	parkedEvents    metric.Int64Counter
)

func init() {
	publishedEvents, _ = meter.Int64Counter("grpcshop.outbox.published", metric.WithDescription("Events confirmed by the broker"))
	parkedEvents, _ = meter.Int64Counter("grpcshop.outbox.parked", metric.WithDescription("Events set aside after too many rejected attempts"))
}

// RegisterBacklogMetrics reports how many events await publishing and how old the
// oldest is: a growing age means the broker is unreachable or the relay is stuck.
// Parked events are reported separately, since only an operator can resolve them.
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
	parked, err := meter.Int64ObservableGauge("grpcshop.outbox.parked_backlog", metric.WithDescription("Parked events awaiting an operator"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		var count, parkedCount int64
		var ageSeconds float64
		err := pool.QueryRow(ctx, `SELECT
				count(*) FILTER (WHERE parked_at IS NULL),
				coalesce(extract(epoch FROM now() - min(created_at) FILTER (WHERE parked_at IS NULL)), 0),
				count(*) FILTER (WHERE parked_at IS NOT NULL)
			FROM outbox WHERE published_at IS NULL`).Scan(&count, &ageSeconds, &parkedCount)
		if err != nil {
			return err
		}
		observer.ObserveInt64(backlog, count)
		observer.ObserveFloat64(oldestAge, ageSeconds)
		observer.ObserveInt64(parked, parkedCount)
		return nil
	}, backlog, oldestAge, parked)
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
	// Publish returns the IDs of messages the broker durably accepted. A nil error
	// with a message missing from the result means the broker rejected it.
	Publish(ctx context.Context, messages []Message) (publishedIDs []string, err error)
}

const (
	// MaxAttempts is how many broker rejections an event gets before it is parked.
	MaxAttempts = 10
	// PublishedRetention is how long published events are kept for debugging.
	PublishedRetention = 7 * 24 * time.Hour

	maxFailureBackoff = 10 * time.Second
	purgeInterval     = time.Hour
	purgeBatchSize    = 1_000
)

type Relay struct {
	pool         *pgxpool.Pool
	publisher    Publisher
	pollInterval time.Duration
	batchSize    int
	// publishTimeout bounds one broker round trip; claimLease must exceed it so a
	// claim never expires while its batch is still being published.
	publishTimeout time.Duration
	claimLease     time.Duration
	logger         *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, publisher Publisher, logger *slog.Logger) *Relay {
	return &Relay{
		pool: pool, publisher: publisher, pollInterval: 500 * time.Millisecond, batchSize: 100,
		publishTimeout: 15 * time.Second, claimLease: time.Minute, logger: logger,
	}
}

func (relay *Relay) Run(ctx context.Context) {
	relay.logger.Info("outbox relay started")
	failureBackoff := relay.pollInterval
	nextPurge := time.Now()
	for {
		if time.Now().After(nextPurge) {
			if purged, err := relay.PurgePublished(ctx, PublishedRetention); err != nil && ctx.Err() == nil {
				relay.logger.Error("outbox purge failed", "error", err)
			} else if purged > 0 {
				relay.logger.Info("purged published events", "count", purged)
			}
			nextPurge = time.Now().Add(purgeInterval)
		}

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

// PurgePublished deletes events published longer ago than retention and returns how many.
func (relay *Relay) PurgePublished(ctx context.Context, retention time.Duration) (int64, error) {
	var total int64
	for {
		// Small batches keep each delete's locks and WAL volume short.
		result, err := relay.pool.Exec(ctx, `DELETE FROM outbox WHERE id IN (
			SELECT id FROM outbox WHERE published_at < now() - make_interval(secs => $1) LIMIT $2)`,
			retention.Seconds(), purgeBatchSize)
		if err != nil {
			return total, err
		}
		total += result.RowsAffected()
		if result.RowsAffected() < purgeBatchSize {
			return total, nil
		}
	}
}

type claimedMessage struct {
	Message
	createdAt time.Time
}

// claimBatch leases the oldest publishable events in its own short transaction,
// so no database transaction stays open while the broker is called.
func (relay *Relay) claimBatch(ctx context.Context) ([]Message, error) {
	// SKIP LOCKED plus the lease lets several order replicas relay concurrently without double-sending a row.
	rows, err := relay.pool.Query(ctx, `
		UPDATE outbox SET claimed_until = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM outbox
			WHERE published_at IS NULL AND parked_at IS NULL AND (claimed_until IS NULL OR claimed_until < now())
			ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id::text, routing_key, payload, request_id, trace_headers, created_at`,
		relay.batchSize, relay.claimLease.Seconds())
	if err != nil {
		return nil, err
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (claimedMessage, error) {
		var message claimedMessage
		err := row.Scan(&message.ID, &message.RoutingKey, &message.Payload, &message.RequestID, &message.TraceHeaders, &message.createdAt)
		return message, err
	})
	if err != nil {
		return nil, err
	}
	// RETURNING does not preserve the subquery's order.
	sort.SliceStable(claimed, func(left, right int) bool { return claimed[left].createdAt.Before(claimed[right].createdAt) })
	messages := make([]Message, len(claimed))
	for index, message := range claimed {
		messages[index] = message.Message
	}
	return messages, nil
}

func (relay *Relay) publishBatch(ctx context.Context) (int, error) {
	messages, err := relay.claimBatch(ctx)
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

	publishCtx, cancel := context.WithTimeout(ctx, relay.publishTimeout)
	publishedIDs, publishErr := relay.publisher.Publish(publishCtx, messages)
	cancel()
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

	unpublishedIDs := make([]string, 0, len(messages)-len(publishedIDs))
	for _, message := range messages {
		if !publishedSet[message.ID] {
			unpublishedIDs = append(unpublishedIDs, message.ID)
		}
	}
	// Recorded even if ctx was cancelled mid-publish, so confirmed events are not resent.
	if err := relay.recordOutcome(context.WithoutCancel(ctx), publishedIDs, unpublishedIDs, publishErr == nil); err != nil {
		return 0, err
	}
	if len(publishedIDs) > 0 {
		relay.logger.Info("published events", "count", len(publishedIDs))
	}
	return len(publishedIDs), publishErr
}

// recordOutcome marks confirmed events published and releases the claim on the rest.
// Only a broker that answered but did not confirm (brokerReachable) counts as an
// attempt: an outage says nothing about the events, so it must never park them.
func (relay *Relay) recordOutcome(ctx context.Context, publishedIDs, unpublishedIDs []string, brokerReachable bool) error {
	return pgx.BeginFunc(ctx, relay.pool, func(transaction pgx.Tx) error {
		if len(publishedIDs) > 0 {
			if _, err := transaction.Exec(ctx,
				"UPDATE outbox SET published_at = now(), claimed_until = NULL WHERE id = ANY($1::uuid[])", publishedIDs); err != nil {
				return err
			}
		}
		if len(unpublishedIDs) == 0 {
			return nil
		}
		if !brokerReachable {
			_, err := transaction.Exec(ctx, "UPDATE outbox SET claimed_until = NULL WHERE id = ANY($1::uuid[])", unpublishedIDs)
			return err
		}
		rows, err := transaction.Query(ctx, `
			UPDATE outbox SET
				attempts = attempts + 1,
				claimed_until = NULL,
				parked_at = CASE WHEN attempts + 1 >= $2 THEN now() END
			WHERE id = ANY($1::uuid[])
			RETURNING id::text, routing_key, parked_at IS NOT NULL`, unpublishedIDs, MaxAttempts)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var eventID, routingKey string
			var parked bool
			if err := rows.Scan(&eventID, &routingKey, &parked); err != nil {
				return err
			}
			if parked {
				parkedEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("routing_key", routingKey)))
				relay.logger.Error("event parked after repeated broker rejections", "event_id", eventID,
					"routing_key", routingKey, "attempts", MaxAttempts)
			}
		}
		return rows.Err()
	})
}
