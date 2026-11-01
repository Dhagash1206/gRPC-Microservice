package order

import (
	"context"
	"embed"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "grpcshop/gen/shop/order/v1"
	"grpcshop/internal/outbox"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

var (
	ErrOrderNotFound = errors.New("order not found")
	// ErrDuplicateRequest means the customer already used this idempotency key.
	ErrDuplicateRequest = errors.New("idempotency key already used")
	// ErrStatusConflict means the order was not in the expected status, e.g. the
	// recovery worker took it over while the request was still running.
	ErrStatusConflict = errors.New("order status changed concurrently")
)

// Transition describes a status change plus the fields that change with it.
type Transition struct {
	From          orderv1.OrderStatus
	To            orderv1.OrderStatus
	FailureReason string
	ReservationID string
	PaymentID     string
	// Event is written to the outbox in the same transaction.
	Event *outbox.Message
}

// PageCursor is the position after the last order of a page (newest first).
type PageCursor struct {
	CreatedAt time.Time
	OrderID   string
}

// InFlightOrder is an order the recovery worker must finish.
type InFlightOrder struct {
	ID         string
	CustomerID string
	TotalCents int64
	Status     orderv1.OrderStatus
}

type Repository interface {
	// CreatePendingOrder returns ErrDuplicateRequest if idempotencyKey (when set) was already used by the customer.
	CreatePendingOrder(ctx context.Context, order *orderv1.Order, idempotencyKey, requestHash string) error
	// FindByIdempotencyKey returns the order and its request hash, or ErrOrderNotFound.
	FindByIdempotencyKey(ctx context.Context, customerID, idempotencyKey string) (*orderv1.Order, string, error)
	// Apply performs the transition atomically; ErrStatusConflict if the order is not in From.
	Apply(ctx context.Context, orderID string, transition Transition) error
	GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error)
	// ListOrders returns up to limit orders older than after (nil = newest).
	ListOrders(ctx context.Context, customerID string, after *PageCursor, limit int) ([]*orderv1.Order, error)
	// ClaimInFlightOrders leases orders stuck in pending/compensating/cancelling for
	// longer than staleAfter; pending ones are moved to compensating.
	ClaimInFlightOrders(ctx context.Context, staleAfter time.Duration, limit int) ([]InFlightOrder, error)
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (repository *PostgresRepository) CreatePendingOrder(ctx context.Context, order *orderv1.Order, idempotencyKey, requestHash string) error {
	return pgx.BeginFunc(ctx, repository.pool, func(transaction pgx.Tx) error {
		result, err := transaction.Exec(ctx, `
			INSERT INTO orders (id, customer_id, total_cents, status, created_at, updated_at, idempotency_key, request_hash)
			VALUES ($1, $2, $3, $4, $5, $5, NULLIF($6, ''), NULLIF($7, ''))
			ON CONFLICT (customer_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
			order.GetId(), order.GetCustomerId(), order.GetTotalCents(), statusName(order.GetStatus()),
			order.GetCreatedAt().AsTime(), idempotencyKey, requestHash)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return ErrDuplicateRequest
		}
		lineRows := make([][]any, len(order.GetLines()))
		for index, line := range order.GetLines() {
			lineRows[index] = []any{order.GetId(), index + 1, line.GetSku(), line.GetProductName(),
				line.GetQuantity(), line.GetUnitPriceCents()}
		}
		_, err = transaction.CopyFrom(ctx, pgx.Identifier{"order_lines"},
			[]string{"order_id", "line_number", "sku", "product_name", "quantity", "unit_price_cents"},
			pgx.CopyFromRows(lineRows))
		return err
	})
}

func (repository *PostgresRepository) Apply(ctx context.Context, orderID string, transition Transition) error {
	return pgx.BeginFunc(ctx, repository.pool, func(transaction pgx.Tx) error {
		// COALESCE(NULLIF(..)) keeps existing values when the transition doesn't set them.
		result, err := transaction.Exec(ctx, `
			UPDATE orders SET
				status = $3,
				failure_reason = COALESCE(NULLIF($4, ''), failure_reason),
				reservation_id = COALESCE(NULLIF($5, ''), reservation_id),
				payment_id = COALESCE(NULLIF($6, ''), payment_id),
				updated_at = now()
			WHERE id = $1 AND status = $2`,
			orderID, statusName(transition.From), statusName(transition.To),
			transition.FailureReason, transition.ReservationID, transition.PaymentID)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return ErrStatusConflict
		}
		if transition.Event == nil {
			return nil
		}
		traceHeaders := transition.Event.TraceHeaders
		if traceHeaders == nil {
			traceHeaders = map[string]string{}
		}
		_, err = transaction.Exec(ctx,
			"INSERT INTO outbox (id, routing_key, payload, request_id, trace_headers) VALUES ($1, $2, $3, $4, $5)",
			transition.Event.ID, transition.Event.RoutingKey, transition.Event.Payload, transition.Event.RequestID, traceHeaders)
		return err
	})
}

const orderColumns = `id::text, customer_id, total_cents, reservation_id, payment_id, created_at, status, failure_reason`

func scanOrder(row pgx.Row) (*orderv1.Order, error) {
	order := &orderv1.Order{}
	var createdAt time.Time
	var status string
	if err := row.Scan(&order.Id, &order.CustomerId, &order.TotalCents, &order.ReservationId, &order.PaymentId,
		&createdAt, &status, &order.FailureReason); err != nil {
		return nil, err
	}
	order.CreatedAt = timestamppb.New(createdAt)
	order.Status = statusFromName(status)
	return order, nil
}

func (repository *PostgresRepository) GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error) {
	if _, err := uuid.Parse(orderID); err != nil {
		return nil, ErrOrderNotFound
	}
	order, err := scanOrder(repository.pool.QueryRow(ctx, "SELECT "+orderColumns+" FROM orders WHERE id = $1", orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, err
	}
	return order, repository.attachLines(ctx, []*orderv1.Order{order})
}

func (repository *PostgresRepository) FindByIdempotencyKey(ctx context.Context, customerID, idempotencyKey string) (*orderv1.Order, string, error) {
	var orderID, requestHash string
	err := repository.pool.QueryRow(ctx,
		"SELECT id::text, coalesce(request_hash, '') FROM orders WHERE customer_id = $1 AND idempotency_key = $2",
		customerID, idempotencyKey).Scan(&orderID, &requestHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", ErrOrderNotFound
	}
	if err != nil {
		return nil, "", err
	}
	order, err := repository.GetOrder(ctx, orderID)
	return order, requestHash, err
}

func (repository *PostgresRepository) ListOrders(ctx context.Context, customerID string, after *PageCursor, limit int) ([]*orderv1.Order, error) {
	var afterCreatedAt *time.Time
	var afterOrderID *string
	if after != nil {
		afterCreatedAt, afterOrderID = &after.CreatedAt, &after.OrderID
	}
	rows, err := repository.pool.Query(ctx, `
		SELECT `+orderColumns+` FROM orders
		WHERE customer_id = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC LIMIT $4`,
		customerID, afterCreatedAt, afterOrderID, limit)
	if err != nil {
		return nil, err
	}
	orders, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*orderv1.Order, error) { return scanOrder(row) })
	if err != nil {
		return nil, err
	}
	return orders, repository.attachLines(ctx, orders)
}

func (repository *PostgresRepository) attachLines(ctx context.Context, orders []*orderv1.Order) error {
	if len(orders) == 0 {
		return nil
	}
	ordersByID := make(map[string]*orderv1.Order, len(orders))
	orderIDs := make([]string, len(orders))
	for index, order := range orders {
		ordersByID[order.GetId()] = order
		orderIDs[index] = order.GetId()
	}
	rows, err := repository.pool.Query(ctx, `
		SELECT order_id::text, sku, product_name, quantity, unit_price_cents
		FROM order_lines WHERE order_id = ANY($1::uuid[]) ORDER BY order_id, line_number`, orderIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var orderID string
		line := &orderv1.OrderLine{}
		if err := rows.Scan(&orderID, &line.Sku, &line.ProductName, &line.Quantity, &line.UnitPriceCents); err != nil {
			return err
		}
		ordersByID[orderID].Lines = append(ordersByID[orderID].Lines, line)
	}
	return rows.Err()
}

func (repository *PostgresRepository) ClaimInFlightOrders(ctx context.Context, staleAfter time.Duration, limit int) ([]InFlightOrder, error) {
	// Bumping updated_at is the lease: other replicas skip the row until it is stale again.
	rows, err := repository.pool.Query(ctx, `
		UPDATE orders SET
			status = CASE WHEN status = 'pending' THEN 'compensating' ELSE status END,
			failure_reason = CASE WHEN status = 'pending' THEN 'checkout did not complete' ELSE failure_reason END,
			updated_at = now()
		WHERE id IN (
			SELECT id FROM orders
			WHERE status IN ('pending', 'compensating', 'cancelling')
			  AND updated_at < now() - make_interval(secs => $1)
			ORDER BY updated_at LIMIT $2 FOR UPDATE SKIP LOCKED)
		RETURNING id::text, customer_id, total_cents, status`, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (InFlightOrder, error) {
		var inFlight InFlightOrder
		var status string
		err := row.Scan(&inFlight.ID, &inFlight.CustomerID, &inFlight.TotalCents, &status)
		inFlight.Status = statusFromName(status)
		return inFlight, err
	})
}
