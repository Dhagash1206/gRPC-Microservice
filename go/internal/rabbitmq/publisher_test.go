package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"grpcshop/internal/outbox"
)

const testQueue = "test.order-events"

var discardedLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// newBroker starts a throwaway RabbitMQ (plaintext, guest login); skipped with -short.
func newBroker(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:4.1-alpine",
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())
}

// openChannel returns a test-side channel, with the consumer queue declared and emptied.
func openChannel(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	connection, err := Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := DeclareConsumerQueue(channel, testQueue, OrderConfirmedRoutingKey); err != nil {
		t.Fatal(err)
	}
	for _, queueName := range []string{testQueue, DeadLetterQueue(testQueue)} {
		if _, err := channel.QueuePurge(queueName, false); err != nil {
			t.Fatal(err)
		}
	}
	return channel
}

func message(id, routingKey string) outbox.Message {
	return outbox.Message{
		ID: id, RoutingKey: routingKey, Payload: []byte("payload-" + id), RequestID: "request-" + id,
		TraceHeaders: map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
}

// getMessage polls, since a confirmed message can take a moment to become visible to basic.get.
func getMessage(t *testing.T, channel *amqp.Channel, queueName string) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		delivery, found, err := channel.Get(queueName, false)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			return delivery
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no message arrived on %s", queueName)
	return amqp.Delivery{}
}

func queueDepth(t *testing.T, channel *amqp.Channel, queueName string) int {
	t.Helper()
	queue, err := channel.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Messages
}

func TestPublisher(t *testing.T) {
	url := newBroker(t)
	ctx := context.Background()

	t.Run("confirmed messages arrive persistent with ids, type and trace headers", func(t *testing.T) {
		channel := openChannel(t, url)
		publisher := NewPublisher(url, nil, discardedLogger)
		t.Cleanup(publisher.Close)

		publishedIDs, err := publisher.Publish(ctx, []outbox.Message{message("event-1", OrderConfirmedRoutingKey)})

		if err != nil || !slices.Equal(publishedIDs, []string{"event-1"}) {
			t.Fatalf("got %v, %v", publishedIDs, err)
		}
		delivery := getMessage(t, channel, testQueue)
		if delivery.MessageId != "event-1" || delivery.Type != OrderConfirmedRoutingKey ||
			delivery.DeliveryMode != amqp.Persistent || string(delivery.Body) != "payload-event-1" {
			t.Fatalf("unexpected delivery: id %q type %q mode %d body %q",
				delivery.MessageId, delivery.Type, delivery.DeliveryMode, delivery.Body)
		}
		if delivery.Headers["x-request-id"] != "request-event-1" || delivery.Headers["traceparent"] == nil {
			t.Fatalf("missing request or trace headers: %v", delivery.Headers)
		}
	})

	t.Run("unroutable messages are not reported as published", func(t *testing.T) {
		channel := openChannel(t, url)
		publisher := NewPublisher(url, nil, discardedLogger)
		t.Cleanup(publisher.Close)

		publishedIDs, err := publisher.Publish(ctx, []outbox.Message{
			message("routable", OrderConfirmedRoutingKey),
			message("unroutable", "order.no-queue-bound"),
		})

		if err != nil || !slices.Equal(publishedIDs, []string{"routable"}) {
			t.Fatalf("got %v, %v; want only the routable event", publishedIDs, err)
		}
		if depth := queueDepth(t, channel, testQueue); depth != 1 {
			t.Fatalf("queue depth %d, want 1", depth)
		}
	})

	t.Run("reconnects after the connection drops", func(t *testing.T) {
		openChannel(t, url)
		publisher := NewPublisher(url, nil, discardedLogger)
		t.Cleanup(publisher.Close)
		if _, err := publisher.Publish(ctx, []outbox.Message{message("before-drop", OrderConfirmedRoutingKey)}); err != nil {
			t.Fatal(err)
		}

		_ = publisher.connection.Close()
		publishedIDs, err := publisher.Publish(ctx, []outbox.Message{message("after-drop", OrderConfirmedRoutingKey)})

		if err != nil || !slices.Equal(publishedIDs, []string{"after-drop"}) {
			t.Fatalf("got %v, %v; want a transparent reconnect", publishedIDs, err)
		}
	})
}

func TestReplayDeadLetters(t *testing.T) {
	url := newBroker(t)
	ctx := context.Background()
	channel := openChannel(t, url)
	publisher := NewPublisher(url, nil, discardedLogger)
	t.Cleanup(publisher.Close)

	if _, err := publisher.Publish(ctx, []outbox.Message{message("poison", OrderConfirmedRoutingKey)}); err != nil {
		t.Fatal(err)
	}
	// Rejecting without requeue dead-letters the message, as the consumer does for poison messages.
	if err := getMessage(t, channel, testQueue).Reject(false); err != nil {
		t.Fatal(err)
	}
	getMessage(t, channel, DeadLetterQueue(testQueue)).Nack(false, true) //nolint:errcheck // peeked only

	replayedCount, err := ReplayDeadLetters(ctx, url, nil, testQueue)

	if err != nil || replayedCount != 1 {
		t.Fatalf("replayed %d, err %v", replayedCount, err)
	}
	if replayed := getMessage(t, channel, testQueue); replayed.MessageId != "poison" || replayed.Type != OrderConfirmedRoutingKey {
		t.Fatalf("replayed message changed: id %q type %q", replayed.MessageId, replayed.Type)
	}
	if depth := queueDepth(t, channel, DeadLetterQueue(testQueue)); depth != 0 {
		t.Fatalf("dead-letter queue still holds %d messages", depth)
	}
}
