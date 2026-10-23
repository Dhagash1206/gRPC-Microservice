// Package migrate applies embedded, forward-only SQL migrations exactly once.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Arbitrary constant; serializes migrations when several replicas start at once.
const advisoryLockID = 727_002

func Apply(ctx context.Context, pool *pgxpool.Pool, migrations fs.FS, logger *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", advisoryLockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	fileNames, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(fileNames)

	for _, fileName := range fileNames {
		var alreadyApplied bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", fileName).
			Scan(&alreadyApplied); err != nil {
			return err
		}
		if alreadyApplied {
			continue
		}
		migrationSQL, err := fs.ReadFile(migrations, fileName)
		if err != nil {
			return err
		}
		transaction, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		// Exec without arguments uses the simple protocol, which allows multiple statements.
		if _, err := transaction.Exec(ctx, string(migrationSQL)); err != nil {
			transaction.Rollback(ctx) //nolint:errcheck
			return fmt.Errorf("migration %s: %w", fileName, err)
		}
		if _, err := transaction.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", fileName); err != nil {
			transaction.Rollback(ctx) //nolint:errcheck
			return err
		}
		if err := transaction.Commit(ctx); err != nil {
			return err
		}
		logger.Info("applied migration", "version", fileName)
	}
	return nil
}
