package order

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "grpcshop/gen/shop/events/v1"
	orderv1 "grpcshop/gen/shop/order/v1"
	"grpcshop/internal/outbox"
	"grpcshop/internal/rabbitmq"
)

func orderConfirmedEvent(ctx context.Context, order *orderv1.Order, requestID string) (outbox.Message, error) {
	var itemCount int32
	for _, line := range order.GetLines() {
		itemCount += line.GetQuantity()
	}
	eventID := uuid.NewString()
	return newOutboxMessage(ctx, eventID, rabbitmq.OrderConfirmedRoutingKey, requestID, &eventsv1.OrderConfirmed{
		EventId:    eventID,
		OrderId:    order.GetId(),
		CustomerId: order.GetCustomerId(),
		TotalCents: order.GetTotalCents(),
		ItemCount:  itemCount,
		OccurredAt: order.GetCreatedAt(),
	})
}

func orderCancelledEvent(ctx context.Context, orderID, customerID string, refundedCents int64, occurredAt time.Time, requestID string) (outbox.Message, error) {
	eventID := uuid.NewString()
	return newOutboxMessage(ctx, eventID, rabbitmq.OrderCancelledRoutingKey, requestID, &eventsv1.OrderCancelled{
		EventId:       eventID,
		OrderId:       orderID,
		CustomerId:    customerID,
		RefundedCents: refundedCents,
		OccurredAt:    timestamppb.New(occurredAt),
	})
}

func newOutboxMessage(ctx context.Context, eventID, routingKey, requestID string, event proto.Message) (outbox.Message, error) {
	payload, err := proto.Marshal(event)
	if err != nil {
		return outbox.Message{}, err
	}
	traceHeaders := map[string]string{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceHeaders))
	return outbox.Message{ID: eventID, RoutingKey: routingKey, Payload: payload, RequestID: requestID, TraceHeaders: traceHeaders}, nil
}
