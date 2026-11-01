package notification

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"grpcshop/internal/rabbitmq"
)

const (
	// Name predates order.cancelled; renaming would orphan the existing queue and its binding.
	QueueName     = "notification.order-confirmed"
	prefetchCount = 16
	maxBackoff    = 30 * time.Second
)

// RunConsumer consumes until ctx is cancelled, reconnecting with backoff.
func RunConsumer(ctx context.Context, rabbitmqURL string, tlsConfig *tls.Config, handler *Handler, logger *slog.Logger) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := consumeUntilDisconnected(ctx, rabbitmqURL, tlsConfig, handler, logger)
		if ctx.Err() != nil {
			return
		}
		logger.Warn("consumer disconnected, retrying", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func consumeUntilDisconnected(ctx context.Context, rabbitmqURL string, tlsConfig *tls.Config, handler *Handler, logger *slog.Logger) error {
	connection, err := rabbitmq.Dial(rabbitmqURL, tlsConfig)
	if err != nil {
		return err
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	if err := rabbitmq.DeclareConsumerQueue(channel, QueueName,
		rabbitmq.OrderConfirmedRoutingKey, rabbitmq.OrderCancelledRoutingKey); err != nil {
		return err
	}
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return err
	}
	deliveries, err := channel.ConsumeWithContext(ctx, QueueName, "notification", false, false, false, false, nil)
	if err != nil {
		return err
	}
	logger.Info("consuming", "queue", QueueName)

	for delivery := range deliveries {
		if err := processDelivery(ctx, delivery, handler); err != nil {
			return err
		}
	}
	return errors.New("delivery channel closed")
}

func processDelivery(ctx context.Context, delivery amqp.Delivery, handler *Handler) error {
	requestID, _ := delivery.Headers["x-request-id"].(string)
	// Dispatch on Type (set by the publisher to the event type): dead-lettered and
	// replayed messages no longer carry the original routing key.
	eventType := delivery.Type
	if eventType == "" {
		eventType = delivery.RoutingKey
	}

	traceCarrier := propagation.MapCarrier{}
	for key, value := range delivery.Headers {
		if text, ok := value.(string); ok {
			traceCarrier[key] = text
		}
	}
	spanCtx, span := tracer.Start(otel.GetTextMapPropagator().Extract(ctx, traceCarrier), "process "+eventType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.message.id", delivery.MessageId)))
	defer span.End()

	outcome := handler.Handle(spanCtx, eventType, delivery.Body, requestID)
	span.SetAttributes(attribute.String("notification.outcome", outcome.String()))
	if outcome != Ack {
		span.SetStatus(codes.Error, outcome.String())
	}
	switch outcome {
	case Ack:
		return delivery.Ack(false)
	case Requeue:
		return delivery.Nack(false, true)
	default:
		return delivery.Nack(false, false)
	}
}
