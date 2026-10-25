package order

import (
	"context"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	inventoryv1 "grpcshop/gen/shop/inventory/v1"
	orderv1 "grpcshop/gen/shop/order/v1"
	paymentv1 "grpcshop/gen/shop/payment/v1"
	"grpcshop/internal/outbox"
)

type fakeInventory struct {
	inventoryv1.InventoryServiceClient // unimplemented methods panic if called

	productsBySKU                               map[string]*inventoryv1.Product
	batchErr, reserveErr, commitErr, releaseErr error
	reservedOrderIDs, committedIDs, releasedIDs []string
}

func (inventory *fakeInventory) BatchGetProducts(_ context.Context, request *inventoryv1.BatchGetProductsRequest, _ ...grpc.CallOption) (*inventoryv1.BatchGetProductsResponse, error) {
	if inventory.batchErr != nil {
		return nil, inventory.batchErr
	}
	response := &inventoryv1.BatchGetProductsResponse{}
	for _, sku := range request.GetSkus() {
		response.Products = append(response.Products, inventory.productsBySKU[sku])
	}
	return response, nil
}

func (inventory *fakeInventory) ReserveStock(_ context.Context, request *inventoryv1.ReserveStockRequest, _ ...grpc.CallOption) (*inventoryv1.ReserveStockResponse, error) {
	inventory.reservedOrderIDs = append(inventory.reservedOrderIDs, request.GetOrderId())
	if inventory.reserveErr != nil {
		return nil, inventory.reserveErr
	}
	return &inventoryv1.ReserveStockResponse{ReservationId: "res_1"}, nil
}

func (inventory *fakeInventory) CommitReservation(_ context.Context, request *inventoryv1.CommitReservationRequest, _ ...grpc.CallOption) (*inventoryv1.CommitReservationResponse, error) {
	inventory.committedIDs = append(inventory.committedIDs, request.GetOrderId())
	if inventory.commitErr != nil {
		return nil, inventory.commitErr
	}
	return &inventoryv1.CommitReservationResponse{}, nil
}

func (inventory *fakeInventory) ReleaseStock(_ context.Context, request *inventoryv1.ReleaseStockRequest, _ ...grpc.CallOption) (*inventoryv1.ReleaseStockResponse, error) {
	if inventory.releaseErr != nil {
		return nil, inventory.releaseErr
	}
	inventory.releasedIDs = append(inventory.releasedIDs, request.GetOrderId())
	return &inventoryv1.ReleaseStockResponse{}, nil
}

type fakePayment struct {
	paymentv1.PaymentServiceClient

	chargeErr, cancelErr error
	chargeRequests       []*paymentv1.ChargeRequest
	cancelledKeys        []string
	// beforeChargeReturns runs inside Charge, e.g. to simulate a concurrent takeover.
	beforeChargeReturns func()
}

func (payment *fakePayment) Charge(_ context.Context, request *paymentv1.ChargeRequest, _ ...grpc.CallOption) (*paymentv1.ChargeResponse, error) {
	payment.chargeRequests = append(payment.chargeRequests, request)
	if payment.beforeChargeReturns != nil {
		payment.beforeChargeReturns()
	}
	if payment.chargeErr != nil {
		return nil, payment.chargeErr
	}
	return &paymentv1.ChargeResponse{PaymentId: "pay_1"}, nil
}

func (payment *fakePayment) CancelCharge(_ context.Context, request *paymentv1.CancelChargeRequest, _ ...grpc.CallOption) (*paymentv1.CancelChargeResponse, error) {
	if payment.cancelErr != nil {
		return nil, payment.cancelErr
	}
	payment.cancelledKeys = append(payment.cancelledKeys, request.GetIdempotencyKey())
	return &paymentv1.CancelChargeResponse{Outcome: paymentv1.CancelOutcome_CANCEL_OUTCOME_REFUNDED}, nil
}

type idempotencyRecord struct {
	orderID     string
	requestHash string
}

// memoryRepository mirrors PostgresRepository's semantics in memory.
type memoryRepository struct {
	mutex            sync.Mutex
	ordersByID       map[string]*orderv1.Order
	idempotencyByKey map[string]idempotencyRecord // customerID + "/" + key
	savedEvents      []outbox.Message
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{ordersByID: map[string]*orderv1.Order{}, idempotencyByKey: map[string]idempotencyRecord{}}
}

func (repository *memoryRepository) CreatePendingOrder(_ context.Context, order *orderv1.Order, idempotencyKey, requestHash string) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	if idempotencyKey != "" {
		scopedKey := order.GetCustomerId() + "/" + idempotencyKey
		if _, used := repository.idempotencyByKey[scopedKey]; used {
			return ErrDuplicateRequest
		}
		repository.idempotencyByKey[scopedKey] = idempotencyRecord{orderID: order.GetId(), requestHash: requestHash}
	}
	repository.ordersByID[order.GetId()] = proto.Clone(order).(*orderv1.Order)
	return nil
}

func (repository *memoryRepository) FindByIdempotencyKey(_ context.Context, customerID, idempotencyKey string) (*orderv1.Order, string, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	record, found := repository.idempotencyByKey[customerID+"/"+idempotencyKey]
	if !found {
		return nil, "", ErrOrderNotFound
	}
	return proto.Clone(repository.ordersByID[record.orderID]).(*orderv1.Order), record.requestHash, nil
}

func (repository *memoryRepository) Apply(_ context.Context, orderID string, transition Transition) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	order, found := repository.ordersByID[orderID]
	if !found || order.GetStatus() != transition.From {
		return ErrStatusConflict
	}
	order.Status = transition.To
	if transition.FailureReason != "" {
		order.FailureReason = transition.FailureReason
	}
	if transition.ReservationID != "" {
		order.ReservationId = transition.ReservationID
	}
	if transition.PaymentID != "" {
		order.PaymentId = transition.PaymentID
	}
	if transition.Event != nil {
		repository.savedEvents = append(repository.savedEvents, *transition.Event)
	}
	return nil
}

func (repository *memoryRepository) GetOrder(_ context.Context, orderID string) (*orderv1.Order, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	if order, found := repository.ordersByID[orderID]; found {
		return proto.Clone(order).(*orderv1.Order), nil
	}
	return nil, ErrOrderNotFound
}

func (repository *memoryRepository) ListOrders(_ context.Context, customerID string, after *PageCursor, limit int) ([]*orderv1.Order, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	var orders []*orderv1.Order
	for _, order := range repository.ordersByID {
		if order.GetCustomerId() == customerID && (after == nil || isBefore(order, *after)) {
			orders = append(orders, proto.Clone(order).(*orderv1.Order))
		}
	}
	sort.Slice(orders, func(i, j int) bool {
		return !isBefore(orders[i], PageCursor{CreatedAt: orders[j].GetCreatedAt().AsTime(), OrderID: orders[j].GetId()})
	})
	return orders[:min(limit, len(orders))], nil
}

// isBefore reports whether order sorts after cursor in newest-first order.
func isBefore(order *orderv1.Order, cursor PageCursor) bool {
	createdAt := order.GetCreatedAt().AsTime()
	return createdAt.Before(cursor.CreatedAt) || (createdAt.Equal(cursor.CreatedAt) && order.GetId() < cursor.OrderID)
}

func (repository *memoryRepository) ClaimInFlightOrders(context.Context, time.Duration, int) ([]InFlightOrder, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	var claimed []InFlightOrder
	for _, order := range repository.ordersByID {
		switch order.GetStatus() {
		case statusPending:
			order.Status = statusCompensating
			order.FailureReason = "checkout did not complete"
		case statusCompensating, statusCancelling:
		default:
			continue
		}
		claimed = append(claimed, InFlightOrder{ID: order.GetId(), CustomerID: order.GetCustomerId(),
			TotalCents: order.GetTotalCents(), Status: order.GetStatus()})
	}
	return claimed, nil
}

func (repository *memoryRepository) status(orderID string) orderv1.OrderStatus {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	return repository.ordersByID[orderID].GetStatus()
}

func (repository *memoryRepository) onlyOrderID() string {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	for orderID := range repository.ordersByID {
		return orderID
	}
	return ""
}
