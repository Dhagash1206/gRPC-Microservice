package order

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "grpcshop/gen/shop/inventory/v1"
	paymentv1 "grpcshop/gen/shop/payment/v1"
)

// RecoveryWorker finishes sagas abandoned by a crashed or stuck request:
//   - stale PENDING orders whose charge succeeded are completed (stock committed,
//     order CONFIRMED), since the customer has paid; if the stock hold has
//     expired meanwhile, they are refunded and FAILED instead
//   - other stale PENDING and COMPENSATING orders are compensated and marked FAILED
//   - stale CANCELLING orders are refunded/restocked and marked CANCELLED
//
// staleAfter must exceed the longest possible CreateOrder duration.
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
	outcome := "compensated"
	if inFlight.Status == statusPending {
		completed, err := worker.completeIfCharged(ctx, inFlight.ID)
		if errors.Is(err, ErrStatusConflict) {
			// The original request finished the saga after all.
			worker.logger.InfoContext(ctx, "order finished concurrently", "order_id", inFlight.ID)
			return false
		}
		if err != nil {
			worker.logger.ErrorContext(ctx, "recover pending order failed; will retry", "order_id", inFlight.ID, "error", err)
			return false // lease expires after staleAfter, then it is retried
		}
		if completed {
			recoveredOrders.Add(ctx, 1, metric.WithAttributes(
				attribute.String("from_status", inFlight.Status.String()), attribute.String("outcome", "completed")))
			return true
		}
		inFlight.Status = statusCompensating
	}

	// The saga step reached is unknown, so undo everything; both calls are idempotent.
	if err := worker.service.compensate(ctx, inFlight.ID, undoEverything, "recovery"); err != nil {
		return false // lease expires after staleAfter, then it is retried
	}
	var finishErr error
	switch inFlight.Status {
	case statusCompensating:
		finishErr = worker.service.repository.Apply(ctx, inFlight.ID, Transition{From: statusCompensating, To: statusFailed})
	case statusCancelling:
		outcome = "cancelled"
		finishErr = worker.service.finishCancellation(ctx, inFlight.ID, inFlight.CustomerID, inFlight.TotalCents, inFlight.CurrencyCode)
	}
	if finishErr != nil {
		worker.logger.ErrorContext(ctx, "finish recovery failed", "order_id", inFlight.ID, "error", finishErr)
		return false
	}
	recoveredOrders.Add(ctx, 1, metric.WithAttributes(
		attribute.String("from_status", inFlight.Status.String()), attribute.String("outcome", outcome)))
	return true
}

// completeIfCharged confirms a PENDING order whose charge succeeded. It returns
// false, with the order moved to COMPENSATING, when the order must be undone
// instead. ErrStatusConflict means the original request got there first; any
// other error leaves the order PENDING for a later retry.
func (worker *RecoveryWorker) completeIfCharged(ctx context.Context, orderID string) (bool, error) {
	service := worker.service
	charge, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*paymentv1.GetChargeResponse, error) {
		return service.payment.GetCharge(callCtx, &paymentv1.GetChargeRequest{IdempotencyKey: orderID})
	})
	charged := err == nil && charge.GetStatus() == paymentv1.ChargeStatus_CHARGE_STATUS_SUCCEEDED
	if err != nil && status.Code(err) != codes.NotFound {
		return false, err
	}
	if !charged {
		return false, worker.startCompensation(ctx, orderID, "checkout did not complete")
	}

	// Charge comes after ReserveStock, so a successful charge means stock was held.
	commit, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*inventoryv1.CommitReservationResponse, error) {
		return service.inventory.CommitReservation(callCtx, &inventoryv1.CommitReservationRequest{OrderId: orderID})
	})
	if isDefiniteFailure(err) {
		return false, worker.startCompensation(ctx, orderID, "stock hold expired before checkout completed")
	}
	if err != nil {
		return false, err
	}

	order, err := service.repository.GetOrder(ctx, orderID)
	if err != nil {
		return false, err
	}
	order.Status = statusConfirmed
	order.ReservationId = commit.GetReservationId()
	order.PaymentId = charge.GetPaymentId()
	event, err := orderConfirmedEvent(ctx, order, "")
	if err != nil {
		return false, err
	}
	err = service.repository.Apply(ctx, orderID, Transition{
		From: statusPending, To: statusConfirmed,
		ReservationID: order.GetReservationId(), PaymentID: order.GetPaymentId(), Event: &event,
	})
	if err != nil {
		return false, err
	}
	worker.logger.InfoContext(ctx, "completed abandoned paid order", "order_id", orderID)
	return true, nil
}

// startCompensation moves a PENDING order to COMPENSATING, which also stops the
// original request from confirming it.
func (worker *RecoveryWorker) startCompensation(ctx context.Context, orderID, reason string) error {
	return worker.service.repository.Apply(ctx, orderID, Transition{From: statusPending, To: statusCompensating, FailureReason: reason})
}
