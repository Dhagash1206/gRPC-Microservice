package notification

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

func Migrations() fs.FS {
	migrations, _ := fs.Sub(embeddedMigrations, "migrations")
	return migrations
}

// ProcessedEvents runs a side effect at most once per event ID (per successful run).
type ProcessedEvents interface {
	// RunOnce calls sideEffect unless eventID was already processed. The event is
	// recorded only if sideEffect succeeds.
	RunOnce(ctx context.Context, eventID, routingKey string, sideEffect func() error) (alreadyProcessed bool, err error)
}

type PostgresProcessedEvents struct {
	pool *pgxpool.Pool
}

func NewPostgresProcessedEvents(pool *pgxpool.Pool) *PostgresProcessedEvents {
	return &PostgresProcessedEvents{pool: pool}
}

var errAlreadyProcessed = errors.New("already processed")

func (events *PostgresProcessedEvents) RunOnce(ctx context.Context, eventID, routingKey string, sideEffect func() error) (bool, error) {
	// The uncommitted insert holds the key, so a concurrent replica handling the
	// same event blocks until this one commits (then skips) or rolls back (then runs).
	err := pgx.BeginFunc(ctx, events.pool, func(transaction pgx.Tx) error {
		result, err := transaction.Exec(ctx,
			"INSERT INTO processed_events (event_id, routing_key) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			eventID, routingKey)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return errAlreadyProcessed
		}
		return sideEffect()
	})
	if errors.Is(err, errAlreadyProcessed) {
		return true, nil
	}
	return false, err
}

// MemoryProcessedEvents is a single-process implementation for tests.
type MemoryProcessedEvents struct {
	mutex     sync.Mutex
	processed map[string]bool
}

func NewMemoryProcessedEvents() *MemoryProcessedEvents {
	return &MemoryProcessedEvents{processed: map[string]bool{}}
}

func (events *MemoryProcessedEvents) RunOnce(_ context.Context, eventID, _ string, sideEffect func() error) (bool, error) {
	events.mutex.Lock()
	defer events.mutex.Unlock()
	if events.processed[eventID] {
		return true, nil
	}
	if err := sideEffect(); err != nil {
		return false, err
	}
	events.processed[eventID] = true
	return false, nil
}
