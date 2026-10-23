package order

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	eventsv1 "grpcshop/gen/shop/events/v1"
	inventoryv1 "grpcshop/gen/shop/inventory/v1"
	orderv1 "grpcshop/gen/shop/order/v1"
	"grpcshop/internal/identity"
	"grpcshop/internal/rabbitmq"
)

var (
	errUnavailable  = status.Error(codes.Unavailable, "connection refused")
	errDeadline     = status.Error(codes.DeadlineExceeded, "deadline exceeded")
	errOutOfStock   = status.Error(codes.FailedPrecondition, "insufficient stock for MOUSE")
	errDeclined     = status.Error(codes.FailedPrecondition, "payment declined")
	errHoldExpired  = status.Error(codes.FailedPrecondition, "reservation for order is missing, released or expired")
	discardedLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
)

type testFixture struct {
	service    *Service
	inventory  *fakeInventory
	payment    *fakePayment
	repository *memoryRepository
	recovery   *RecoveryWorker
}

func newFixture() *testFixture {
	inventory := &fakeInventory{productsBySKU: map[string]*inventoryv1.Product{
		"KEYBOARD": {Sku: "KEYBOARD", Name: "Keyboard", UnitPriceCents: 4_999},
		"MOUSE":    {Sku: "MOUSE", Name: "Mouse", UnitPriceCents: 1_999},
	}}
	payment := &fakePayment{}
	repository := newMemoryRepository()
	service := NewService(inventory, payment, repository, time.Second, discardedLogger)
	return &testFixture{
		service:    service,
		inventory:  inventory,
		payment:    payment,
		repository: repository,
		recovery:   NewRecoveryWorker(service, time.Minute, time.Second, discardedLogger),
	}
}

func customerContext(customerID string) context.Context {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + customerID + `"}`))
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(identity.JWTPayloadMetadataKey, payload))
}

func createRequest(quantitiesBySKU map[string]int32) *orderv1.CreateOrderRequest {
	request := &orderv1.CreateOrderRequest{}
	for sku, quantity := range quantitiesBySKU {
		request.Items = append(request.Items, &orderv1.OrderItem{Sku: sku, Quantity: quantity})
	}
	return request
}

func (fixture *testFixture) createMouseOrder(t *testing.T, customerID string) *orderv1.Order {
	t.Helper()
	order, err := fixture.service.CreateOrder(customerContext(customerID), createRequest(map[string]int32{"MOUSE": 1}))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	return order
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("got code %v (%v), want %v", got, err, want)
	}
}

func assertStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
}

func TestCreateOrderConfirmsAndEmitsEvent(t *testing.T) {
	fixture := newFixture()

	order, err := fixture.service.CreateOrder(customerContext("customer-1"),
		createRequest(map[string]int32{"KEYBOARD": 2, "MOUSE": 1}))

	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	wantTotal := int64(2*4_999 + 1_999)
	if order.GetStatus() != statusConfirmed || order.GetTotalCents() != wantTotal || order.GetPaymentId() != "pay_1" {
		t.Fatalf("unexpected order: %v", order)
	}
	if fixture.repository.status(order.GetId()) != statusConfirmed {
		t.Fatal("stored order not confirmed")
	}
	charge := fixture.payment.chargeRequests[0]
	if charge.GetIdempotencyKey() != order.GetId() || charge.GetAmountCents() != wantTotal {
		t.Fatalf("unexpected charge: %v", charge)
	}
	assertStrings(t, "committed", fixture.inventory.committedIDs, []string{order.GetId()})

	event := &eventsv1.OrderConfirmed{}
	savedEvent := fixture.repository.savedEvents[0]
	if err := proto.Unmarshal(savedEvent.Payload, event); err != nil {
		t.Fatal(err)
	}
	if savedEvent.RoutingKey != rabbitmq.OrderConfirmedRoutingKey || event.GetOrderId() != order.GetId() || event.GetItemCount() != 3 {
		t.Fatalf("unexpected event: %v", event)
	}
}

func TestCreateOrderRequiresIdentity(t *testing.T) {
	_, err := newFixture().service.CreateOrder(context.Background(), createRequest(map[string]int32{"MOUSE": 1}))
	assertCode(t, err, codes.Unauthenticated)
}

func TestCreateOrderValidatesItems(t *testing.T) {
	tooManySKUs := &orderv1.CreateOrderRequest{}
	for index := range MaxDistinctSKUs + 1 {
		tooManySKUs.Items = append(tooManySKUs.Items, &orderv1.OrderItem{Sku: string(rune('A'+index%26)) + string(rune('a'+index/26)), Quantity: 1})
	}
	// Each line is individually valid; the merged total would wrap int32 to 1.
	overflowing := &orderv1.CreateOrderRequest{Items: []*orderv1.OrderItem{
		{Sku: "MOUSE", Quantity: 2147483647}, {Sku: "MOUSE", Quantity: 2147483647}, {Sku: "MOUSE", Quantity: 3},
	}}
	testCases := map[string]*orderv1.CreateOrderRequest{
		"no items":            {},
		"zero quantity":       createRequest(map[string]int32{"MOUSE": 0}),
		"empty sku":           createRequest(map[string]int32{"": 1}),
		"sku with nul byte":   createRequest(map[string]int32{"MO\x00USE": 1}),
		"sku too long":        createRequest(map[string]int32{strings.Repeat("A", 65): 1}),
		"quantity over limit": createRequest(map[string]int32{"MOUSE": MaxQuantityPerSKU + 1}),
		"int32 overflow":      overflowing,
		"too many skus":       tooManySKUs,
	}
	for name, request := range testCases {
		t.Run(name, func(t *testing.T) {
			fixture := newFixture()
			_, err := fixture.service.CreateOrder(customerContext("customer-1"), request)
			assertCode(t, err, codes.InvalidArgument)
			if len(fixture.inventory.reservedOrderIDs) != 0 {
				t.Fatal("inventory must not be called for invalid requests")
			}
		})
	}
}

func TestCreateOrderMergesDuplicateSKUs(t *testing.T) {
	fixture := newFixture()
	request := &orderv1.CreateOrderRequest{Items: []*orderv1.OrderItem{{Sku: "MOUSE", Quantity: 1}, {Sku: "MOUSE", Quantity: 2}}}

	order, err := fixture.service.CreateOrder(customerContext("customer-1"), request)

	if err != nil || len(order.GetLines()) != 1 || order.GetLines()[0].GetQuantity() != 3 {
		t.Fatalf("got %v, %v", order, err)
	}
}

// Each failure point must undo exactly what may have happened, and record why.
func TestCreateOrderFailurePaths(t *testing.T) {
	testCases := []struct {
		name          string
		inject        func(*testFixture)
		wantCode      codes.Code
		wantCancelled bool // CancelCharge called
		wantReleased  bool // ReleaseStock called
	}{
		{"out of stock reserves nothing", func(f *testFixture) { f.inventory.reserveErr = errOutOfStock }, codes.FailedPrecondition, false, false},
		{"reserve outcome unknown is fenced", func(f *testFixture) { f.inventory.reserveErr = errDeadline }, codes.Unavailable, false, true},
		{"declined payment releases stock", func(f *testFixture) { f.payment.chargeErr = errDeclined }, codes.FailedPrecondition, false, true},
		{"charge outcome unknown is cancelled", func(f *testFixture) { f.payment.chargeErr = errUnavailable }, codes.Unavailable, true, true},
		{"expired hold refunds payment", func(f *testFixture) { f.inventory.commitErr = errHoldExpired }, codes.FailedPrecondition, true, true},
		{"commit outcome unknown undoes all", func(f *testFixture) { f.inventory.commitErr = errDeadline }, codes.Unavailable, true, true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newFixture()
			testCase.inject(fixture)

			_, err := fixture.service.CreateOrder(customerContext("customer-1"), createRequest(map[string]int32{"MOUSE": 1}))

			assertCode(t, err, testCase.wantCode)
			orderID := fixture.repository.onlyOrderID()
			if got := fixture.repository.status(orderID); got != statusFailed {
				t.Fatalf("order status %v, want FAILED", got)
			}
			if (len(fixture.payment.cancelledKeys) > 0) != testCase.wantCancelled {
				t.Fatalf("CancelCharge called=%v, want %v", len(fixture.payment.cancelledKeys) > 0, testCase.wantCancelled)
			}
			if (len(fixture.inventory.releasedIDs) > 0) != testCase.wantReleased {
				t.Fatalf("ReleaseStock called=%v, want %v", len(fixture.inventory.releasedIDs) > 0, testCase.wantReleased)
			}
			if len(fixture.repository.savedEvents) != 0 {
				t.Fatal("failed orders must not emit events")
			}
		})
	}
}

func TestInfrastructureErrorsDoNotLeakDetails(t *testing.T) {
	fixture := newFixture()
	fixture.inventory.batchErr = errUnavailable

	_, err := fixture.service.CreateOrder(customerContext("customer-1"), createRequest(map[string]int32{"MOUSE": 1}))

	assertCode(t, err, codes.Unavailable)
	if status.Convert(err).Message() != "inventory service unavailable" {
		t.Fatalf("internal details leaked: %v", err)
	}
}

func TestFailedCompensationLeavesOrderForRecovery(t *testing.T) {
	fixture := newFixture()
	fixture.payment.chargeErr = errUnavailable
	fixture.inventory.releaseErr = errUnavailable

	_, err := fixture.service.CreateOrder(customerContext("customer-1"), createRequest(map[string]int32{"MOUSE": 1}))
	assertCode(t, err, codes.Unavailable)
	orderID := fixture.repository.onlyOrderID()
	if got := fixture.repository.status(orderID); got != statusCompensating {
		t.Fatalf("status %v, want COMPENSATING", got)
	}

	fixture.inventory.releaseErr = nil
	if finished := fixture.recovery.RecoverOnce(context.Background()); finished != 1 {
		t.Fatalf("recovered %d orders, want 1", finished)
	}
	if got := fixture.repository.status(orderID); got != statusFailed {
		t.Fatalf("status %v, want FAILED", got)
	}
}

func TestRecoveryUndoesAbandonedPendingOrder(t *testing.T) {
	fixture := newFixture()
	order := fixture.service.newPendingOrder("customer-1", []*inventoryv1.Product{{Sku: "MOUSE", UnitPriceCents: 1_999}}, map[string]int32{"MOUSE": 1})
	if err := fixture.repository.CreatePendingOrder(context.Background(), order, "", ""); err != nil {
		t.Fatal(err)
	}

	fixture.recovery.RecoverOnce(context.Background())

	if got := fixture.repository.status(order.GetId()); got != statusFailed {
		t.Fatalf("status %v, want FAILED", got)
	}
	assertStrings(t, "cancelled charges", fixture.payment.cancelledKeys, []string{order.GetId()})
	assertStrings(t, "released", fixture.inventory.releasedIDs, []string{order.GetId()})
}

func TestConfirmLosesRaceAgainstRecovery(t *testing.T) {
	fixture := newFixture()
	// Recovery claims the order while the request is still waiting on payment.
	fixture.payment.beforeChargeReturns = func() { fixture.recovery.RecoverOnce(context.Background()) }

	_, err := fixture.service.CreateOrder(customerContext("customer-1"), createRequest(map[string]int32{"MOUSE": 1}))

	assertCode(t, err, codes.Aborted)
	if got := fixture.repository.status(fixture.repository.onlyOrderID()); got != statusFailed {
		t.Fatalf("status %v, want FAILED", got)
	}
	if len(fixture.repository.savedEvents) != 0 {
		t.Fatal("rolled-back order must not emit OrderConfirmed")
	}
}

func TestCancelOrderRefundsRestocksAndIsIdempotent(t *testing.T) {
	fixture := newFixture()
	order := fixture.createMouseOrder(t, "owner")

	for range 2 {
		cancelled, err := fixture.service.CancelOrder(customerContext("owner"), &orderv1.CancelOrderRequest{Id: order.GetId()})
		if err != nil || cancelled.GetStatus() != statusCancelled {
			t.Fatalf("got %v, %v", cancelled, err)
		}
	}
	assertStrings(t, "cancelled charges", fixture.payment.cancelledKeys, []string{order.GetId()})
	assertStrings(t, "released", fixture.inventory.releasedIDs, []string{order.GetId()})
	if lastEvent := fixture.repository.savedEvents[len(fixture.repository.savedEvents)-1]; lastEvent.RoutingKey != rabbitmq.OrderCancelledRoutingKey {
		t.Fatalf("last event %q, want order.cancelled", lastEvent.RoutingKey)
	}
}

func TestCancelOrderInterruptedIsFinishedByRecovery(t *testing.T) {
	fixture := newFixture()
	order := fixture.createMouseOrder(t, "owner")
	fixture.payment.cancelErr = errUnavailable

	_, err := fixture.service.CancelOrder(customerContext("owner"), &orderv1.CancelOrderRequest{Id: order.GetId()})
	assertCode(t, err, codes.Unavailable)
	if got := fixture.repository.status(order.GetId()); got != statusCancelling {
		t.Fatalf("status %v, want CANCELLING", got)
	}

	fixture.payment.cancelErr = nil
	fixture.recovery.RecoverOnce(context.Background())
	if got := fixture.repository.status(order.GetId()); got != statusCancelled {
		t.Fatalf("status %v, want CANCELLED", got)
	}
}

func TestCancelOrderRejectsOtherCustomersAndFailedOrders(t *testing.T) {
	fixture := newFixture()
	order := fixture.createMouseOrder(t, "owner")
	_, err := fixture.service.CancelOrder(customerContext("intruder"), &orderv1.CancelOrderRequest{Id: order.GetId()})
	assertCode(t, err, codes.NotFound)

	fixture.payment.chargeErr = errDeclined
	_, _ = fixture.service.CreateOrder(customerContext("owner"), createRequest(map[string]int32{"MOUSE": 1}))
	orders, _ := fixture.service.ListOrders(customerContext("owner"), &orderv1.ListOrdersRequest{})
	for _, listed := range orders.GetOrders() {
		if listed.GetStatus() == statusFailed {
			_, err = fixture.service.CancelOrder(customerContext("owner"), &orderv1.CancelOrderRequest{Id: listed.GetId()})
			assertCode(t, err, codes.FailedPrecondition)
			return
		}
	}
	t.Fatal("expected a failed order")
}

func TestGetOrderHidesOtherCustomersOrders(t *testing.T) {
	fixture := newFixture()
	order := fixture.createMouseOrder(t, "owner")

	if _, err := fixture.service.GetOrder(customerContext("owner"), &orderv1.GetOrderRequest{Id: order.GetId()}); err != nil {
		t.Fatalf("owner lookup failed: %v", err)
	}
	_, err := fixture.service.GetOrder(customerContext("someone-else"), &orderv1.GetOrderRequest{Id: order.GetId()})
	assertCode(t, err, codes.NotFound)
}

func TestListOrdersPaginatesNewestFirst(t *testing.T) {
	fixture := newFixture()
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixture.service.now = func() time.Time { clock = clock.Add(time.Second); return clock }
	var createdIDs []string
	for range 5 {
		createdIDs = append(createdIDs, fixture.createMouseOrder(t, "owner").GetId())
	}
	fixture.createMouseOrder(t, "someone-else")

	var listedIDs []string
	pageToken := ""
	for pageCount := 1; ; pageCount++ {
		page, err := fixture.service.ListOrders(customerContext("owner"), &orderv1.ListOrdersRequest{PageSize: 2, PageToken: pageToken})
		if err != nil {
			t.Fatal(err)
		}
		for _, order := range page.GetOrders() {
			listedIDs = append(listedIDs, order.GetId())
		}
		if pageToken = page.GetNextPageToken(); pageToken == "" {
			if pageCount != 3 {
				t.Fatalf("got %d pages, want 3", pageCount)
			}
			break
		}
	}
	slices.Reverse(createdIDs)
	assertStrings(t, "listed", listedIDs, createdIDs)
}

func TestListOrdersRejectsTamperedPageToken(t *testing.T) {
	_, err := newFixture().service.ListOrders(customerContext("owner"), &orderv1.ListOrdersRequest{PageToken: "not-a-token"})
	assertCode(t, err, codes.InvalidArgument)
}

func keyedRequest(idempotencyKey string, quantitiesBySKU map[string]int32) *orderv1.CreateOrderRequest {
	request := createRequest(quantitiesBySKU)
	request.IdempotencyKey = idempotencyKey
	return request
}

func TestIdempotencyKeyReplaysConfirmedOrderWithoutRecharging(t *testing.T) {
	fixture := newFixture()
	ctx := customerContext("customer-1")

	first, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 2}))
	if err != nil {
		t.Fatal(err)
	}
	retried, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 2}))
	if err != nil {
		t.Fatal(err)
	}

	if retried.GetId() != first.GetId() || len(fixture.payment.chargeRequests) != 1 || len(fixture.inventory.reservedOrderIDs) != 1 {
		t.Fatalf("retry was not a replay: ids %s/%s, charges %d", first.GetId(), retried.GetId(), len(fixture.payment.chargeRequests))
	}
}

func TestIdempotencyKeyIsScopedPerCustomer(t *testing.T) {
	fixture := newFixture()
	first, _ := fixture.service.CreateOrder(customerContext("alice"), keyedRequest("shared", map[string]int32{"MOUSE": 1}))
	second, err := fixture.service.CreateOrder(customerContext("bob"), keyedRequest("shared", map[string]int32{"MOUSE": 1}))
	if err != nil || second.GetId() == first.GetId() || second.GetCustomerId() != "bob" {
		t.Fatalf("bob got %v, %v", second, err)
	}
}

func TestIdempotencyKeyReuseWithDifferentItemsIsRejected(t *testing.T) {
	fixture := newFixture()
	ctx := customerContext("customer-1")
	if _, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 1})); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 5}))
	assertCode(t, err, codes.InvalidArgument)
}

func TestIdempotencyKeyRetryDuringFirstAttemptIsAborted(t *testing.T) {
	fixture := newFixture()
	ctx := customerContext("customer-1")
	var concurrentErr error
	fixture.payment.beforeChargeReturns = func() {
		fixture.payment.beforeChargeReturns = nil
		_, concurrentErr = fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 1}))
	}

	if _, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 1})); err != nil {
		t.Fatal(err)
	}
	assertCode(t, concurrentErr, codes.Aborted)
}

func TestIdempotencyKeyAfterFailureAsksForNewKey(t *testing.T) {
	fixture := newFixture()
	ctx := customerContext("customer-1")
	fixture.payment.chargeErr = errDeclined
	_, _ = fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 1}))

	fixture.payment.chargeErr = nil
	_, err := fixture.service.CreateOrder(ctx, keyedRequest("key-1", map[string]int32{"MOUSE": 1}))
	assertCode(t, err, codes.FailedPrecondition)
	if _, err := fixture.service.CreateOrder(ctx, keyedRequest("key-2", map[string]int32{"MOUSE": 1})); err != nil {
		t.Fatalf("new key should succeed: %v", err)
	}
}

func TestInvalidIdempotencyKeyIsRejected(t *testing.T) {
	_, err := newFixture().service.CreateOrder(customerContext("customer-1"),
		keyedRequest("bad key\n", map[string]int32{"MOUSE": 1}))
	assertCode(t, err, codes.InvalidArgument)
}
