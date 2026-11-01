package order

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	orderv1 "grpcshop/gen/shop/order/v1"
)

// RecoveryWorker finishes sagas abandoned by a crashed or stuck request:
// stale PENDING/COMPENSATING orders are compensated and marked FAILED, stale
// CANCELLING orders are refunded/restocked and marked CANCELLED. staleAfter must
// exceed the longest possible CreateOrder duration.
type RecoveryWorker struct {
	service    *Service
	staleAfter time.Duration
	interval   time.Duration
	logger     *slog.Logger
}

func NewRecoveryWorker(service *Service, staleAfter, interval time.Duration, logger *slog.Logger) *RecoveryWorker {
	return &RecoveryWorker{service: service, staleAfter: staleAfter, interval: interval, logger: logger}
}

func (worker *RecoveryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(worker.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.RecoverOnce(ctx)
		}
	}
}

// RecoverOnce processes one batch and returns how many orders reached a final status.
func (worker *RecoveryWorker) RecoverOnce(ctx context.Context) int {
	inFlightOrders, err := worker.service.repository.ClaimInFlightOrders(ctx, worker.staleAfter, 50)
	if err != nil {
		worker.logger.ErrorContext(ctx, "claim in-flight orders failed", "error", err)
		return 0
	}
	finishedCount := 0
	for _, inFlight := range inFlightOrders {
		if worker.recover(ctx, inFlight) {
			finishedCount++
		}
	}
	return finishedCount
}

func (worker *RecoveryWorker) recover(ctx context.Context, inFlight InFlightOrder) bool {
	ctx, span := tracer.Start(ctx, "recover order", trace.WithAttributes(
		attribute.String("order.id", inFlight.ID), attribute.String("order.status", inFlight.Status.String())))
	defer span.End()

	worker.logger.WarnContext(ctx, "recovering abandoned order", "order_id", inFlight.ID, "status", inFlight.Status.String())
	// The saga step reached is unknown, so undo everything; both calls are idempotent.
	if err := worker.service.compensate(ctx, inFlight.ID, undoEverything, "recovery"); err != nil {
		return false // lease expires after staleAfter, then it is retried
	}
	var finishErr error
	switch inFlight.Status {
	case orderv1.OrderStatus_ORDER_STATUS_COMPENSATING:
		finishErr = worker.service.repository.Apply(ctx, inFlight.ID, Transition{From: statusCompensating, To: statusFailed})
	case orderv1.OrderStatus_ORDER_STATUS_CANCELLING:
		finishErr = worker.service.finishCancellation(ctx, inFlight.ID, inFlight.CustomerID, inFlight.TotalCents)
	}
	if finishErr != nil {
		worker.logger.ErrorContext(ctx, "finish recovery failed", "order_id", inFlight.ID, "error", finishErr)
		return false
	}
	recoveredOrders.Add(ctx, 1, metric.WithAttributes(attribute.String("from_status", inFlight.Status.String())))
	return true
}
