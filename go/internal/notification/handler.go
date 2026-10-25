// Package notification consumes OrderConfirmed events and notifies customers.
package notification

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	eventsv1 "grpcshop/gen/shop/events/v1"
	"grpcshop/internal/rabbitmq"
)

type Outcome int

const (
	Ack        Outcome = iota // processed (or duplicate)
	Requeue                   // transient failure; broker redelivers up to its delivery limit
	DeadLetter                // poison message; retrying cannot help
)

func (outcome Outcome) String() string {
	return [...]string{"ack", "requeue", "dead_letter"}[outcome]
}

var (
	tracer                = otel.Tracer("grpcshop/notification")
	notificationsTotal, _ = otel.Meter("grpcshop/notification").Int64Counter("grpcshop.notifications",
		metric.WithDescription("Processed events by type and result: sent, duplicate, requeued, dead_lettered"))
)

func recordNotification(ctx context.Context, eventType, result string) {
	notificationsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventType), attribute.String("result", result)))
}

// Notifier stands in for an email/SMS provider.
type Notifier interface {
	SendOrderConfirmation(ctx context.Context, event *eventsv1.OrderConfirmed) error
	SendOrderCancellation(ctx context.Context, event *eventsv1.OrderCancelled) error
}

type LogNotifier struct {
	Logger *slog.Logger
}

func (notifier LogNotifier) SendOrderConfirmation(ctx context.Context, event *eventsv1.OrderConfirmed) error {
	notifier.Logger.InfoContext(ctx, "sending order confirmation",
		"customer_id", event.GetCustomerId(), "order_id", event.GetOrderId(),
		"total_cents", event.GetTotalCents(), "item_count", event.GetItemCount())
	return nil
}

func (notifier LogNotifier) SendOrderCancellation(ctx context.Context, event *eventsv1.OrderCancelled) error {
	notifier.Logger.InfoContext(ctx, "sending cancellation notice",
		"customer_id", event.GetCustomerId(), "order_id", event.GetOrderId(), "refunded_cents", event.GetRefundedCents())
	return nil
}

// Transient failures are retried in-process before the message is requeued: an
// immediate requeue is redelivered within milliseconds, so a short SMTP outage
// would exhaust the queue's delivery limit and dead-letter the email.
var retryBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}

type Handler struct {
	notifier        Notifier
	processedEvents ProcessedEvents
	logger          *slog.Logger
	sleep           func(ctx context.Context, duration time.Duration) error
}

func NewHandler(notifier Notifier, processedEvents ProcessedEvents, logger *slog.Logger) *Handler {
	return &Handler{notifier: notifier, processedEvents: processedEvents, logger: logger, sleep: sleepContext}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Handle dispatches one delivery by routing key.
func (handler *Handler) Handle(ctx context.Context, routingKey string, body []byte, requestID string) Outcome {
	switch routingKey {
	case rabbitmq.OrderConfirmedRoutingKey:
		event := &eventsv1.OrderConfirmed{}
		return handler.handle(ctx, routingKey, body, event, requestID, func() string { return event.GetEventId() },
			func() error { return handler.notifier.SendOrderConfirmation(ctx, event) })
	case rabbitmq.OrderCancelledRoutingKey:
		event := &eventsv1.OrderCancelled{}
		return handler.handle(ctx, routingKey, body, event, requestID, func() string { return event.GetEventId() },
			func() error { return handler.notifier.SendOrderCancellation(ctx, event) })
	default:
		handler.logger.ErrorContext(ctx, "unknown routing key, dead-lettering", "routing_key", routingKey, "rid", requestID)
		recordNotification(ctx, "unknown", "dead_lettered")
		return DeadLetter
	}
}

func (handler *Handler) handle(ctx context.Context, routingKey string, body []byte, event proto.Message, requestID string,
	eventID func() string, notify func() error) Outcome {
	if err := proto.Unmarshal(body, event); err != nil || uuid.Validate(eventID()) != nil {
		handler.logger.ErrorContext(ctx, "undecodable event, dead-lettering", "rid", requestID)
		recordNotification(ctx, routingKey, "dead_lettered")
		return DeadLetter
	}
	// Delivery is at-least-once; the durable record makes redeliveries no-ops.
	alreadyProcessed, err := handler.processedEvents.RunOnce(ctx, eventID(), routingKey, notify)
	for attempt := 0; err != nil; attempt++ {
		if attempt == len(retryBackoffs) {
			handler.logger.WarnContext(ctx, "notification failed, requeueing", "event_id", eventID(), "error", err)
			recordNotification(ctx, routingKey, "requeued")
			return Requeue
		}
		handler.logger.WarnContext(ctx, "notification failed, retrying", "event_id", eventID(),
			"retry_in", retryBackoffs[attempt].String(), "error", err)
		if handler.sleep(ctx, retryBackoffs[attempt]) != nil {
			return Requeue // shutting down
		}
		alreadyProcessed, err = handler.processedEvents.RunOnce(ctx, eventID(), routingKey, notify)
	}
	if alreadyProcessed {
		handler.logger.InfoContext(ctx, "duplicate event skipped", "event_id", eventID(), "rid", requestID)
		recordNotification(ctx, routingKey, "duplicate")
		return Ack
	}
	handler.logger.InfoContext(ctx, "event handled", "event_id", eventID(), "routing_key", routingKey, "rid", requestID)
	recordNotification(ctx, routingKey, "sent")
	return Ack
}
