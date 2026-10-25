package order

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "grpcshop/gen/shop/order/v1"
	"grpcshop/internal/migrate"
	"grpcshop/internal/outbox"
)

// newPostgresRepository starts a throwaway Postgres; skipped with -short.
func newPostgresRepository(t *testing.T) (*PostgresRepository, *pgxpool.Pool) {
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
	if err := migrate.Apply(ctx, pool, Migrations(), discardedLogger); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Re-applying must be a no-op.
	if err := migrate.Apply(ctx, pool, Migrations(), discardedLogger); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	return NewPostgresRepository(pool), pool
}

func pendingOrder(customerID string, createdAt time.Time) *orderv1.Order {
	return &orderv1.Order{
		Id:         uuid.NewString(),
		CustomerId: customerID,
		Status:     statusPending,
		TotalCents: 1_999,
		CreatedAt:  timestamppb.New(createdAt.Truncate(time.Microsecond)),
		Lines:      []*orderv1.OrderLine{{Sku: "MOUSE", ProductName: "Mouse", Quantity: 1, UnitPriceCents: 1_999}},
	}
}

func TestPostgresRepository(t *testing.T) {
	repository, pool := newPostgresRepository(t)
	ctx := context.Background()

	t.Run("transition is conditional and writes outbox atomically", func(t *testing.T) {
		order := pendingOrder("customer-1", time.Now())
		if err := repository.CreatePendingOrder(ctx, order, "", ""); err != nil {
			t.Fatal(err)
		}
		event := outbox.Message{ID: uuid.NewString(), RoutingKey: "order.confirmed", Payload: []byte("x")}
		confirm := Transition{From: statusPending, To: statusConfirmed, ReservationID: "res_1", PaymentID: "pay_1", Event: &event}

		if err := repository.Apply(ctx, order.GetId(), confirm); err != nil {
			t.Fatal(err)
		}
		if err := repository.Apply(ctx, order.GetId(), confirm); !errors.Is(err, ErrStatusConflict) {
			t.Fatalf("second confirm: got %v, want ErrStatusConflict", err)
		}

		stored, err := repository.GetOrder(ctx, order.GetId())
		if err != nil {
			t.Fatal(err)
		}
		if stored.GetStatus() != statusConfirmed || stored.GetPaymentId() != "pay_1" || len(stored.GetLines()) != 1 ||
			!stored.GetCreatedAt().AsTime().Equal(order.GetCreatedAt().AsTime()) {
			t.Fatalf("unexpected stored order: %v", stored)
		}
		var outboxCount int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE id = $1", event.ID).Scan(&outboxCount); err != nil || outboxCount != 1 {
			t.Fatalf("outbox rows=%d err=%v, want exactly 1", outboxCount, err)
		}
	})

	t.Run("idempotency key is unique per customer", func(t *testing.T) {
		first := pendingOrder("idem", time.Now())
		if err := repository.CreatePendingOrder(ctx, first, "key-1", "hash-a"); err != nil {
			t.Fatal(err)
		}
		if err := repository.CreatePendingOrder(ctx, pendingOrder("idem", time.Now()), "key-1", "hash-a"); !errors.Is(err, ErrDuplicateRequest) {
			t.Fatalf("duplicate insert: got %v, want ErrDuplicateRequest", err)
		}
		if err := repository.CreatePendingOrder(ctx, pendingOrder("someone-else", time.Now()), "key-1", "hash-a"); err != nil {
			t.Fatalf("other customer, same key: %v", err)
		}
		// Orders without a key never conflict with each other.
		for range 2 {
			if err := repository.CreatePendingOrder(ctx, pendingOrder("idem", time.Now()), "", ""); err != nil {
				t.Fatal(err)
			}
		}
		found, requestHash, err := repository.FindByIdempotencyKey(ctx, "idem", "key-1")
		if err != nil || found.GetId() != first.GetId() || requestHash != "hash-a" {
			t.Fatalf("FindByIdempotencyKey: %v %q %v", found.GetId(), requestHash, err)
		}
	})

	t.Run("unknown or malformed ids are not found", func(t *testing.T) {
		for _, orderID := range []string{uuid.NewString(), "not-a-uuid", ""} {
			if _, err := repository.GetOrder(ctx, orderID); !errors.Is(err, ErrOrderNotFound) {
				t.Fatalf("GetOrder(%q): got %v", orderID, err)
			}
		}
	})

	t.Run("keyset pagination is stable with identical timestamps", func(t *testing.T) {
		sameInstant := time.Now()
		var createdIDs []string
		for range 5 {
			order := pendingOrder("pager", sameInstant)
			if err := repository.CreatePendingOrder(ctx, order, "", ""); err != nil {
				t.Fatal(err)
			}
			createdIDs = append(createdIDs, order.GetId())
		}
		var listedIDs []string
		var cursor *PageCursor
		for {
			page, err := repository.ListOrders(ctx, "pager", cursor, 2)
			if err != nil {
				t.Fatal(err)
			}
			for _, order := range page {
				listedIDs = append(listedIDs, order.GetId())
			}
			if len(page) < 2 {
				break
			}
			last := page[len(page)-1]
			cursor = &PageCursor{CreatedAt: last.GetCreatedAt().AsTime(), OrderID: last.GetId()}
		}
		slices.Sort(createdIDs)
		slices.Reverse(createdIDs) // equal timestamps fall back to id DESC
		if !slices.Equal(listedIDs, createdIDs) {
			t.Fatalf("listed %v, want %v", listedIDs, createdIDs)
		}
	})

	t.Run("claim leases only stale in-flight orders", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "UPDATE orders SET status = 'failed' WHERE status = 'pending'"); err != nil {
			t.Fatal(err)
		}
		stale := pendingOrder("claimer", time.Now())
		fresh := pendingOrder("claimer", time.Now())
		for _, order := range []*orderv1.Order{stale, fresh} {
			if err := repository.CreatePendingOrder(ctx, order, "", ""); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, "UPDATE orders SET updated_at = now() - interval '5 minutes' WHERE id = $1", stale.GetId()); err != nil {
			t.Fatal(err)
		}

		claimed, err := repository.ClaimInFlightOrders(ctx, time.Minute, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 || claimed[0].ID != stale.GetId() || claimed[0].Status != statusCompensating {
			t.Fatalf("claimed %+v, want only the stale order as COMPENSATING", claimed)
		}
		// The lease (fresh updated_at) hides it from an immediate second claim.
		if reclaimed, _ := repository.ClaimInFlightOrders(ctx, time.Minute, 10); len(reclaimed) != 0 {
			t.Fatalf("reclaimed %+v during lease", reclaimed)
		}
		// A request finishing late cannot confirm a claimed order.
		err = repository.Apply(ctx, stale.GetId(), Transition{From: statusPending, To: statusConfirmed})
		if !errors.Is(err, ErrStatusConflict) {
			t.Fatalf("late confirm: got %v, want ErrStatusConflict", err)
		}
	})
}
