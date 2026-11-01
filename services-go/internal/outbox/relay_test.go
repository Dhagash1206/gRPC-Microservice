package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"grpcshop/internal/migrate"
	"grpcshop/internal/order"
	"grpcshop/internal/outbox"
)

var discardedLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

var errBrokerDown = errors.New("broker unavailable")

// fakePublisher confirms the messages chosen by confirm (all of them by default).
type fakePublisher struct {
	batches [][]outbox.Message
	confirm func(messages []outbox.Message) ([]string, error)
}

func (publisher *fakePublisher) Publish(_ context.Context, messages []outbox.Message) ([]string, error) {
	publisher.batches = append(publisher.batches, slices.Clone(messages))
	if publisher.confirm != nil {
		return publisher.confirm(messages)
	}
	ids := make([]string, len(messages))
	for index, message := range messages {
		ids[index] = message.ID
	}
	return ids, nil
}

// newOrderDatabase starts a throwaway Postgres with the order schema; skipped with -short.
func newOrderDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("order"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	connectionString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrate.Apply(ctx, pool, order.Migrations(), discardedLogger); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

type outboxRow struct {
	published bool
	attempts  int
}

// insertEvent adds an outbox row; age orders rows, since the relay publishes oldest first.
func insertEvent(t *testing.T, pool *pgxpool.Pool, age time.Duration, published bool, traceHeaders map[string]string) string {
	t.Helper()
	if traceHeaders == nil {
		traceHeaders = map[string]string{}
	}
	headersJSON, err := json.Marshal(traceHeaders)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	_, err = pool.Exec(context.Background(), `
		INSERT INTO outbox (id, routing_key, payload, request_id, created_at, published_at, trace_headers)
		VALUES ($1, 'order.confirmed', 'payload', 'request-1', now() - $2::interval,
		        CASE WHEN $3 THEN now() END, $4)`,
		eventID, age.String(), published, headersJSON)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

func readRow(t *testing.T, pool *pgxpool.Pool, eventID string) outboxRow {
	t.Helper()
	var row outboxRow
	err := pool.QueryRow(context.Background(),
		"SELECT published_at IS NOT NULL, attempts FROM outbox WHERE id = $1", eventID).Scan(&row.published, &row.attempts)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func clearOutbox(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "DELETE FROM outbox"); err != nil {
		t.Fatal(err)
	}
}

func publishedIDs(batch []outbox.Message) []string {
	ids := make([]string, len(batch))
	for index, message := range batch {
		ids[index] = message.ID
	}
	return ids
}

func TestRelay(t *testing.T) {
	pool := newOrderDatabase(t)
	ctx := context.Background()

	t.Run("publishes pending events oldest first, exactly once", func(t *testing.T) {
		clearOutbox(t, pool)
		newer := insertEvent(t, pool, time.Minute, false, nil)
		older := insertEvent(t, pool, time.Hour, false, nil)
		alreadyPublished := insertEvent(t, pool, 2*time.Hour, true, nil)
		publisher := &fakePublisher{}
		relay := outbox.NewRelay(pool, publisher, discardedLogger)

		count, err := relay.PublishBatch(ctx)
		if err != nil || count != 2 {
			t.Fatalf("first batch: count %d, err %v", count, err)
		}
		if got := publishedIDs(publisher.batches[0]); !slices.Equal(got, []string{older, newer}) {
			t.Fatalf("published %v, want %v (oldest first, published row skipped)", got, []string{older, newer})
		}
		for _, eventID := range []string{older, newer, alreadyPublished} {
			if !readRow(t, pool, eventID).published {
				t.Fatalf("event %s not marked published", eventID)
			}
		}

		count, err = relay.PublishBatch(ctx)
		if err != nil || count != 0 || len(publisher.batches) != 1 {
			t.Fatalf("second batch must find nothing: count %d, err %v, publisher calls %d", count, err, len(publisher.batches))
		}
	})

	t.Run("unconfirmed events stay pending with an extra attempt", func(t *testing.T) {
		clearOutbox(t, pool)
		confirmedEvent := insertEvent(t, pool, time.Hour, false, nil)
		unconfirmedEvent := insertEvent(t, pool, time.Minute, false, nil)
		publisher := &fakePublisher{confirm: func([]outbox.Message) ([]string, error) {
			return []string{confirmedEvent}, errBrokerDown
		}}

		count, err := outbox.NewRelay(pool, publisher, discardedLogger).PublishBatch(ctx)

		if count != 1 || !errors.Is(err, errBrokerDown) {
			t.Fatalf("got count %d, err %v; want 1 and the broker error", count, err)
		}
		if row := readRow(t, pool, confirmedEvent); !row.published || row.attempts != 0 {
			t.Fatalf("confirmed event: %+v", row)
		}
		if row := readRow(t, pool, unconfirmedEvent); row.published || row.attempts != 1 {
			t.Fatalf("unconfirmed event: %+v, want pending with 1 attempt", row)
		}
	})

	t.Run("broker failure leaves every event pending for the next batch", func(t *testing.T) {
		clearOutbox(t, pool)
		eventIDs := []string{insertEvent(t, pool, time.Hour, false, nil), insertEvent(t, pool, time.Minute, false, nil)}
		failing := &fakePublisher{confirm: func([]outbox.Message) ([]string, error) { return nil, errBrokerDown }}

		count, err := outbox.NewRelay(pool, failing, discardedLogger).PublishBatch(ctx)
		if count != 0 || !errors.Is(err, errBrokerDown) {
			t.Fatalf("got count %d, err %v", count, err)
		}
		for _, eventID := range eventIDs {
			if row := readRow(t, pool, eventID); row.published || row.attempts != 1 {
				t.Fatalf("event %s: %+v, want pending with 1 attempt", eventID, row)
			}
		}

		recovered := &fakePublisher{}
		count, err = outbox.NewRelay(pool, recovered, discardedLogger).PublishBatch(ctx)
		if err != nil || count != 2 {
			t.Fatalf("after recovery: count %d, err %v", count, err)
		}
	})

	t.Run("publish span continues the trace of the request that wrote the event", func(t *testing.T) {
		clearOutbox(t, pool)
		spanRecorder := tracetest.NewSpanRecorder()
		previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
		otel.SetTextMapPropagator(propagation.TraceContext{})
		t.Cleanup(func() {
			otel.SetTracerProvider(previousProvider)
			otel.SetTextMapPropagator(previousPropagator)
		})

		requestTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
		requestSpanID := "00f067aa0ba902b7"
		insertEvent(t, pool, time.Minute, false, map[string]string{"traceparent": "00-" + requestTraceID + "-" + requestSpanID + "-01"})
		publisher := &fakePublisher{}

		if _, err := outbox.NewRelay(pool, publisher, discardedLogger).PublishBatch(ctx); err != nil {
			t.Fatal(err)
		}

		spans := spanRecorder.Ended()
		if len(spans) != 1 || spans[0].Name() != "publish order.confirmed" || spans[0].SpanKind() != trace.SpanKindProducer {
			t.Fatalf("want one producer span, got %d", len(spans))
		}
		publishSpan := spans[0]
		if publishSpan.SpanContext().TraceID().String() != requestTraceID || publishSpan.Parent().SpanID().String() != requestSpanID {
			t.Fatalf("span not parented to the request: trace %s parent %s", publishSpan.SpanContext().TraceID(), publishSpan.Parent().SpanID())
		}
		// Consumers must become children of the publish span, not of the original request span.
		wantHeader := "00-" + requestTraceID + "-" + publishSpan.SpanContext().SpanID().String() + "-01"
		if got := publisher.batches[0][0].TraceHeaders["traceparent"]; got != wantHeader {
			t.Fatalf("traceparent sent to broker = %q, want %q", got, wantHeader)
		}
	})
}
