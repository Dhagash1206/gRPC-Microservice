// Package order implements OrderService. CreateOrder runs a saga whose state is
// persisted before any remote side effect, so every outcome is recoverable:
//
//  1. Inventory.BatchGetProducts -> prices; store the order as PENDING
//  2. Inventory.ReserveStock     -> hold stock (idempotent on order ID, expires unless committed)
//  3. Payment.Charge             -> idempotent on order ID
//  4. Inventory.CommitReservation
//  5. PENDING -> CONFIRMED + OrderConfirmed outbox event, in one transaction
//
// On failure the order moves to COMPENSATING, the charge is cancelled and the
// stock released (both fence late-arriving calls), then it becomes FAILED. If
// this process dies mid-saga, RecoveryWorker finishes the compensation.
package order

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "grpcshop/gen/shop/inventory/v1"
	orderv1 "grpcshop/gen/shop/order/v1"
	paymentv1 "grpcshop/gen/shop/payment/v1"
	"grpcshop/internal/grpcmw"
	"grpcshop/internal/identity"
)

const (
	MaxDistinctSKUs    = 50
	MaxQuantityPerSKU  = 1_000
	defaultPageSize    = 20
	maxPageSize        = 100
	statusPending      = orderv1.OrderStatus_ORDER_STATUS_PENDING
	statusConfirmed    = orderv1.OrderStatus_ORDER_STATUS_CONFIRMED
	statusCompensating = orderv1.OrderStatus_ORDER_STATUS_COMPENSATING
	statusFailed       = orderv1.OrderStatus_ORDER_STATUS_FAILED
	statusCancelling   = orderv1.OrderStatus_ORDER_STATUS_CANCELLING
	statusCancelled    = orderv1.OrderStatus_ORDER_STATUS_CANCELLED
)

// Codes meaning the downstream call definitely had no effect, so its message is
// safe to return to our caller. Anything else (timeouts, crashes, cancellation)
// leaves the outcome unknown and is reported as UNAVAILABLE.
var definiteFailureCodes = map[codes.Code]bool{
	codes.InvalidArgument:    true,
	codes.NotFound:           true,
	codes.FailedPrecondition: true,
}

// compensation selects which side effects to undo.
type compensation struct {
	cancelCharge bool
	releaseStock bool
}

var undoEverything = compensation{cancelCharge: true, releaseStock: true}

type Service struct {
	orderv1.UnimplementedOrderServiceServer

	inventory         inventoryv1.InventoryServiceClient
	payment           paymentv1.PaymentServiceClient
	repository        Repository
	downstreamTimeout time.Duration
	logger            *slog.Logger
	now               func() time.Time
}

func NewService(
	inventory inventoryv1.InventoryServiceClient,
	payment paymentv1.PaymentServiceClient,
	repository Repository,
	downstreamTimeout time.Duration,
	logger *slog.Logger,
) *Service {
	return &Service{
		inventory:         inventory,
		payment:           payment,
		repository:        repository,
		downstreamTimeout: downstreamTimeout,
		logger:            logger,
		now:               time.Now,
	}
}

func (service *Service) CreateOrder(ctx context.Context, request *orderv1.CreateOrderRequest) (*orderv1.Order, error) {
	customerID, err := identity.CustomerID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	sortedSKUs, quantitiesBySKU, err := mergeItems(request.GetItems())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	idempotencyKey := request.GetIdempotencyKey()
	if idempotencyKey != "" && !validIdempotencyKey.MatchString(idempotencyKey) {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key must be 1-128 characters of [A-Za-z0-9_-]")
	}
	requestHash := hashItems(sortedSKUs, quantitiesBySKU)
	if idempotencyKey != "" {
		// Retries normally land here, before any downstream call is repeated.
		if _, _, err := service.repository.FindByIdempotencyKey(ctx, customerID, idempotencyKey); err == nil {
			return service.replayDuplicate(ctx, customerID, idempotencyKey, requestHash)
		} else if !errors.Is(err, ErrOrderNotFound) {
			return nil, status.Error(codes.Internal, "failed to check idempotency key")
		}
	}

	productsResponse, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*inventoryv1.BatchGetProductsResponse, error) {
		return service.inventory.BatchGetProducts(callCtx, &inventoryv1.BatchGetProductsRequest{Skus: sortedSKUs})
	})
	if err != nil {
		return nil, service.downstreamError(ctx, "inventory", err)
	}

	order := service.newPendingOrder(customerID, productsResponse.GetProducts(), quantitiesBySKU)
	err = service.repository.CreatePendingOrder(ctx, order, idempotencyKey, requestHash)
	if errors.Is(err, ErrDuplicateRequest) {
		// A concurrent request with the same key won the insert.
		return service.replayDuplicate(ctx, customerID, idempotencyKey, requestHash)
	}
	if err != nil {
		service.logger.ErrorContext(ctx, "store pending order failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to create order")
	}
	orderID := order.GetId()

	stockItems := make([]*inventoryv1.StockItem, len(sortedSKUs))
	for index, sku := range sortedSKUs {
		stockItems[index] = &inventoryv1.StockItem{Sku: sku, Quantity: quantitiesBySKU[sku]}
	}
	reservation, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*inventoryv1.ReserveStockResponse, error) {
		return service.inventory.ReserveStock(callCtx, &inventoryv1.ReserveStockRequest{OrderId: orderID, Items: stockItems})
	})
	if err != nil {
		// A definite rejection reserved nothing; an unknown outcome needs a fencing release.
		return nil, service.failCheckout(ctx, orderID, "inventory", err, compensation{releaseStock: !isDefiniteFailure(err)})
	}

	charge, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*paymentv1.ChargeResponse, error) {
		return service.payment.Charge(callCtx, &paymentv1.ChargeRequest{
			IdempotencyKey: orderID, CustomerId: customerID, AmountCents: order.GetTotalCents(),
		})
	})
	if err != nil {
		return nil, service.failCheckout(ctx, orderID, "payment", err,
			compensation{cancelCharge: !isDefiniteFailure(err), releaseStock: true})
	}

	_, err = callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*inventoryv1.CommitReservationResponse, error) {
		return service.inventory.CommitReservation(callCtx, &inventoryv1.CommitReservationRequest{OrderId: orderID})
	})
	if err != nil {
		return nil, service.failCheckout(ctx, orderID, "inventory", err, undoEverything)
	}

	order.Status = statusConfirmed
	order.ReservationId = reservation.GetReservationId()
	order.PaymentId = charge.GetPaymentId()
	event, err := orderConfirmedEvent(ctx, order, grpcmw.RequestIDFromContext(ctx))
	if err != nil {
		return nil, service.failCheckout(ctx, orderID, "order", err, undoEverything)
	}
	err = service.repository.Apply(context.WithoutCancel(ctx), orderID, Transition{
		From: statusPending, To: statusConfirmed,
		ReservationID: order.GetReservationId(), PaymentID: order.GetPaymentId(), Event: &event,
	})
	if errors.Is(err, ErrStatusConflict) {
		// The recovery worker already took this order over and is undoing it.
		recordCheckout(ctx, "aborted")
		return nil, status.Error(codes.Aborted, "checkout timed out and was rolled back")
	}
	if err != nil {
		service.logger.ErrorContext(ctx, "confirm order failed", "order_id", orderID, "error", err)
		return nil, service.failCheckout(ctx, orderID, "order", status.Error(codes.Internal, "confirm failed"), undoEverything)
	}

	service.logger.InfoContext(ctx, "order confirmed", "order_id", orderID, "customer_id", customerID,
		"total_cents", order.GetTotalCents(), "rid", grpcmw.RequestIDFromContext(ctx))
	recordCheckout(ctx, "confirmed")
	return order, nil
}

// replayDuplicate answers a repeated CreateOrder with the outcome of the first attempt.
func (service *Service) replayDuplicate(ctx context.Context, customerID, idempotencyKey, requestHash string) (*orderv1.Order, error) {
	existing, existingHash, err := service.repository.FindByIdempotencyKey(ctx, customerID, idempotencyKey)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to load original request")
	}
	recordCheckout(ctx, "replayed")
	if existingHash != requestHash {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key was already used with different items")
	}
	switch existing.GetStatus() {
	case statusPending, statusCompensating:
		return nil, status.Error(codes.Aborted, "the original request with this idempotency_key is still in progress")
	case statusFailed:
		return nil, status.Errorf(codes.FailedPrecondition,
			"the original request with this idempotency_key failed (%s); retry with a new key", existing.GetFailureReason())
	default: // confirmed, or later cancelled: the order was created
		return existing, nil
	}
}

func (service *Service) GetOrder(ctx context.Context, request *orderv1.GetOrderRequest) (*orderv1.Order, error) {
	customerID, err := identity.CustomerID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return service.loadOwnedOrder(ctx, request.GetId(), customerID)
}

func (service *Service) ListOrders(ctx context.Context, request *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	customerID, err := identity.CustomerID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	pageSize := int(request.GetPageSize())
	switch {
	case pageSize < 0:
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	case pageSize == 0:
		pageSize = defaultPageSize
	case pageSize > maxPageSize:
		pageSize = maxPageSize
	}
	cursor, err := decodePageToken(request.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}

	// Fetch one extra row to learn whether another page exists.
	orders, err := service.repository.ListOrders(ctx, customerID, cursor, pageSize+1)
	if err != nil {
		service.logger.ErrorContext(ctx, "list orders failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to list orders")
	}
	response := &orderv1.ListOrdersResponse{Orders: orders}
	if len(orders) > pageSize {
		response.Orders = orders[:pageSize]
		lastOrder := response.Orders[pageSize-1]
		response.NextPageToken = encodePageToken(PageCursor{CreatedAt: lastOrder.GetCreatedAt().AsTime(), OrderID: lastOrder.GetId()})
	}
	return response, nil
}

func (service *Service) CancelOrder(ctx context.Context, request *orderv1.CancelOrderRequest) (*orderv1.Order, error) {
	customerID, err := identity.CustomerID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	order, err := service.loadOwnedOrder(ctx, request.GetId(), customerID)
	if err != nil {
		return nil, err
	}

	switch order.GetStatus() {
	case statusCancelled:
		return order, nil
	case statusConfirmed:
		err := service.repository.Apply(ctx, order.GetId(), Transition{From: statusConfirmed, To: statusCancelling})
		if errors.Is(err, ErrStatusConflict) {
			return nil, status.Error(codes.Aborted, "order changed concurrently; retry")
		}
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to cancel order")
		}
	case statusCancelling:
		// A retry of an interrupted cancellation: finish it.
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "only confirmed orders can be cancelled (status %s)", order.GetStatus())
	}

	cancelCtx := context.WithoutCancel(ctx)
	if err := service.compensate(cancelCtx, order.GetId(), undoEverything, "cancel"); err != nil {
		// The recovery worker retries CANCELLING orders, so this completes eventually.
		return nil, status.Error(codes.Unavailable, "cancellation in progress; it will complete automatically")
	}
	if err := service.finishCancellation(cancelCtx, order.GetId(), order.GetCustomerId(), order.GetTotalCents()); err != nil &&
		!errors.Is(err, ErrStatusConflict) {
		return nil, status.Error(codes.Internal, "failed to record cancellation")
	}
	return service.loadOwnedOrder(cancelCtx, order.GetId(), customerID)
}

func (service *Service) loadOwnedOrder(ctx context.Context, orderID, customerID string) (*orderv1.Order, error) {
	order, err := service.repository.GetOrder(ctx, orderID)
	// Another customer's order is reported as NOT_FOUND so IDs can't be probed.
	if errors.Is(err, ErrOrderNotFound) || (err == nil && order.GetCustomerId() != customerID) {
		return nil, status.Errorf(codes.NotFound, "order %q not found", orderID)
	}
	if err != nil {
		service.logger.ErrorContext(ctx, "get order failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to load order")
	}
	return order, nil
}

func (service *Service) newPendingOrder(customerID string, products []*inventoryv1.Product, quantitiesBySKU map[string]int32) *orderv1.Order {
	order := &orderv1.Order{
		Id:         uuid.NewString(),
		CustomerId: customerID,
		Status:     statusPending,
		// Postgres stores microseconds; truncate so every read returns the same value.
		CreatedAt: timestamppb.New(service.now().Truncate(time.Microsecond)),
	}
	for _, product := range products {
		quantity := quantitiesBySKU[product.GetSku()]
		order.Lines = append(order.Lines, &orderv1.OrderLine{
			Sku:            product.GetSku(),
			ProductName:    product.GetName(),
			Quantity:       quantity,
			UnitPriceCents: product.GetUnitPriceCents(),
		})
		order.TotalCents += int64(quantity) * product.GetUnitPriceCents()
	}
	return order
}

// failCheckout records the failure, undoes the selected side effects and returns
// the error for the caller. If undoing fails, the order stays COMPENSATING and
// the recovery worker retries.
func (service *Service) failCheckout(ctx context.Context, orderID, downstreamName string, cause error, undo compensation) error {
	ctx = context.WithoutCancel(ctx) // a client disconnect must not interrupt compensation
	reason := status.Convert(cause).Message()
	if isDefiniteFailure(cause) {
		recordCheckout(ctx, "rejected")
	} else {
		recordCheckout(ctx, "failed")
	}

	if undo == (compensation{}) {
		if err := service.repository.Apply(ctx, orderID, Transition{From: statusPending, To: statusFailed, FailureReason: reason}); err != nil {
			service.logger.ErrorContext(ctx, "mark order failed", "order_id", orderID, "error", err)
		}
		return service.downstreamError(ctx, downstreamName, cause)
	}

	err := service.repository.Apply(ctx, orderID, Transition{From: statusPending, To: statusCompensating, FailureReason: reason})
	if err != nil {
		// ErrStatusConflict: the recovery worker already owns this order.
		service.logger.WarnContext(ctx, "could not start compensation", "order_id", orderID, "error", err)
		return service.downstreamError(ctx, downstreamName, cause)
	}
	if err := service.compensate(ctx, orderID, undo, "checkout"); err == nil {
		if err := service.repository.Apply(ctx, orderID, Transition{From: statusCompensating, To: statusFailed}); err != nil {
			service.logger.ErrorContext(ctx, "mark order failed", "order_id", orderID, "error", err)
		}
	}
	return service.downstreamError(ctx, downstreamName, cause)
}

// compensate cancels the charge (refund or fence) before releasing stock. Both
// calls are idempotent, so it is safe to repeat.
func (service *Service) compensate(ctx context.Context, orderID string, undo compensation, trigger string) (err error) {
	ctx, span := tracer.Start(ctx, "compensate", trace.WithAttributes(
		attribute.String("order.id", orderID), attribute.String("compensation.trigger", trigger)))
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
			span.SetStatus(otelcodes.Error, err.Error())
		}
		compensations.Add(ctx, 1, metric.WithAttributes(attribute.String("trigger", trigger), attribute.String("result", result)))
		span.End()
	}()
	if undo.cancelCharge {
		response, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*paymentv1.CancelChargeResponse, error) {
			return service.payment.CancelCharge(callCtx, &paymentv1.CancelChargeRequest{IdempotencyKey: orderID})
		})
		if err != nil {
			service.logger.ErrorContext(ctx, "cancel charge failed; will retry", "order_id", orderID, "error", err)
			return err
		}
		service.logger.InfoContext(ctx, "charge cancelled", "order_id", orderID, "outcome", response.GetOutcome().String())
	}
	if undo.releaseStock {
		if _, err := callWithTimeout(ctx, service.downstreamTimeout, func(callCtx context.Context) (*inventoryv1.ReleaseStockResponse, error) {
			return service.inventory.ReleaseStock(callCtx, &inventoryv1.ReleaseStockRequest{OrderId: orderID})
		}); err != nil {
			service.logger.ErrorContext(ctx, "release stock failed; will retry", "order_id", orderID, "error", err)
			return err
		}
	}
	return nil
}

func (service *Service) finishCancellation(ctx context.Context, orderID, customerID string, refundedCents int64) error {
	event, err := orderCancelledEvent(ctx, orderID, customerID, refundedCents, service.now(), grpcmw.RequestIDFromContext(ctx))
	if err != nil {
		return err
	}
	err = service.repository.Apply(ctx, orderID, Transition{From: statusCancelling, To: statusCancelled, Event: &event})
	if err == nil {
		service.logger.InfoContext(ctx, "order cancelled", "order_id", orderID, "refunded_cents", refundedCents)
	}
	return err
}

func (service *Service) downstreamError(ctx context.Context, downstreamName string, err error) error {
	downstreamStatus := status.Convert(err)
	if definiteFailureCodes[downstreamStatus.Code()] {
		return status.Error(downstreamStatus.Code(), downstreamStatus.Message())
	}
	service.logger.WarnContext(ctx, "downstream call failed", "service", downstreamName,
		"code", downstreamStatus.Code().String(), "message", downstreamStatus.Message())
	if downstreamName == "order" {
		return status.Error(codes.Internal, "failed to complete order")
	}
	return status.Errorf(codes.Unavailable, "%s service unavailable", downstreamName)
}

func isDefiniteFailure(err error) bool {
	return definiteFailureCodes[status.Code(err)]
}

// callWithTimeout bounds a downstream call; the incoming deadline still applies if it is sooner.
func callWithTimeout[Response any](ctx context.Context, timeout time.Duration, call func(context.Context) (Response, error)) (Response, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return call(callCtx)
}

func mergeItems(items []*orderv1.OrderItem) ([]string, map[string]int32, error) {
	if len(items) == 0 {
		return nil, nil, errors.New("order must contain at least one item")
	}
	// Summed as int64 and bounded, so repeated SKUs cannot overflow int32.
	totalsBySKU := map[string]int64{}
	for _, item := range items {
		if item.GetSku() == "" {
			return nil, nil, errors.New("sku is required")
		}
		// Checked before any message echoes the SKU; Postgres also rejects NUL bytes with an internal error.
		if !validSKU.MatchString(item.GetSku()) {
			return nil, nil, errors.New("sku must be 1-64 characters of [A-Za-z0-9._-]")
		}
		if item.GetQuantity() <= 0 {
			return nil, nil, fmt.Errorf("quantity for %s must be positive", item.GetSku())
		}
		totalsBySKU[item.GetSku()] += int64(item.GetQuantity())
		if totalsBySKU[item.GetSku()] > MaxQuantityPerSKU {
			return nil, nil, fmt.Errorf("quantity for %s exceeds %d", item.GetSku(), MaxQuantityPerSKU)
		}
		if len(totalsBySKU) > MaxDistinctSKUs {
			return nil, nil, fmt.Errorf("order may contain at most %d distinct SKUs", MaxDistinctSKUs)
		}
	}
	sortedSKUs := make([]string, 0, len(totalsBySKU))
	quantitiesBySKU := make(map[string]int32, len(totalsBySKU))
	for sku, total := range totalsBySKU {
		sortedSKUs = append(sortedSKUs, sku)
		quantitiesBySKU[sku] = int32(total)
	}
	sort.Strings(sortedSKUs)
	return sortedSKUs, quantitiesBySKU, nil
}

var (
	validIdempotencyKey = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	validSKU            = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// hashItems fingerprints the merged items so a reused idempotency key can be checked.
func hashItems(sortedSKUs []string, quantitiesBySKU map[string]int32) string {
	digest := sha256.New()
	for _, sku := range sortedSKUs {
		fmt.Fprintf(digest, "%s\x00%d\x00", sku, quantitiesBySKU[sku])
	}
	return hex.EncodeToString(digest.Sum(nil))
}

type pageTokenPayload struct {
	CreatedAtMicros int64  `json:"t"`
	OrderID         string `json:"id"`
}

func encodePageToken(cursor PageCursor) string {
	payload, _ := json.Marshal(pageTokenPayload{CreatedAtMicros: cursor.CreatedAt.UnixMicro(), OrderID: cursor.OrderID})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodePageToken(token string) (*PageCursor, error) {
	if token == "" {
		return nil, nil
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var payload pageTokenPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(payload.OrderID); err != nil {
		return nil, err
	}
	return &PageCursor{CreatedAt: time.UnixMicro(payload.CreatedAtMicros), OrderID: payload.OrderID}, nil
}
