package notification

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	eventsv1 "grpcshop/gen/shop/events/v1"
	"grpcshop/internal/rabbitmq"
)

type recordingNotifier struct {
	sentOrderIDs      []string
	cancelledOrderIDs []string
	failNext          bool
	failTimes         int
}

func (notifier *recordingNotifier) SendOrderCancellation(_ context.Context, event *eventsv1.OrderCancelled) error {
	notifier.cancelledOrderIDs = append(notifier.cancelledOrderIDs, event.GetOrderId())
	return nil
}

func (notifier *recordingNotifier) SendOrderConfirmation(_ context.Context, event *eventsv1.OrderConfirmed) error {
	if notifier.failNext || notifier.failTimes > 0 {
		notifier.failNext = false
		notifier.failTimes--
		return errors.New("smtp timeout")
	}
	notifier.sentOrderIDs = append(notifier.sentOrderIDs, event.GetOrderId())
	return nil
}

const firstEventID = "3f8e9c1a-5b7d-4e2f-9a6c-1d0b2e4f6a8c"

func newTestHandler() (*Handler, *recordingNotifier) {
	notifier := &recordingNotifier{}
	handler := NewHandler(notifier, NewMemoryProcessedEvents(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler.sleep = func(context.Context, time.Duration) error { return nil }
	return handler, notifier
}

func encodedEvent(t *testing.T, eventID string) []byte {
	t.Helper()
	body, err := proto.Marshal(&eventsv1.OrderConfirmed{EventId: eventID, OrderId: "order"})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDuplicateEventsNotifyOnce(t *testing.T) {
	handler, notifier := newTestHandler()
	body := encodedEvent(t, firstEventID)

	for range 3 {
		if outcome := handler.Handle(context.Background(), rabbitmq.OrderConfirmedRoutingKey, body, "rid"); outcome != Ack {
			t.Fatalf("got outcome %v, want Ack", outcome)
		}
	}
	if len(notifier.sentOrderIDs) != 1 {
		t.Fatalf("sent %d notifications, want 1", len(notifier.sentOrderIDs))
	}
}

func TestUndecodableEventIsDeadLettered(t *testing.T) {
	handler, _ := newTestHandler()
	if outcome := handler.Handle(context.Background(), rabbitmq.OrderConfirmedRoutingKey, []byte{0xff, 0xff}, "rid"); outcome != DeadLetter {
		t.Fatalf("got %v, want DeadLetter", outcome)
	}
}

func TestTransientFailureIsRetriedInProcess(t *testing.T) {
	handler, notifier := newTestHandler()
	var waits []time.Duration
	handler.sleep = func(_ context.Context, duration time.Duration) error {
		waits = append(waits, duration)
		return nil
	}
	notifier.failTimes = 3

	if outcome := handler.Handle(context.Background(), rabbitmq.OrderConfirmedRoutingKey, encodedEvent(t, firstEventID), "rid"); outcome != Ack {
		t.Fatalf("got %v, want Ack after retries", outcome)
	}
	if len(notifier.sentOrderIDs) != 1 || len(waits) != 3 || waits[2] != 4*time.Second {
		t.Fatalf("sent=%d waits=%v", len(notifier.sentOrderIDs), waits)
	}
}

func TestPersistentFailureIsRequeuedAfterRetries(t *testing.T) {
	handler, notifier := newTestHandler()
	notifier.failTimes = 1_000

	if outcome := handler.Handle(context.Background(), rabbitmq.OrderConfirmedRoutingKey, encodedEvent(t, firstEventID), "rid"); outcome != Requeue {
		t.Fatalf("got %v, want Requeue", outcome)
	}
}

func TestShutdownDuringBackoffRequeues(t *testing.T) {
	handler, notifier := newTestHandler()
	handler.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	notifier.failTimes = 1

	if outcome := handler.Handle(context.Background(), rabbitmq.OrderConfirmedRoutingKey, encodedEvent(t, firstEventID), "rid"); outcome != Requeue {
		t.Fatalf("got %v, want Requeue", outcome)
	}
}

func TestCancellationEventsAreRoutedToCancellationNotice(t *testing.T) {
	handler, notifier := newTestHandler()
	body, err := proto.Marshal(&eventsv1.OrderCancelled{EventId: "9b2c1d55-0d2a-4d0b-8f7e-2f9f7a3c1e10", OrderId: "order-9"})
	if err != nil {
		t.Fatal(err)
	}

	if outcome := handler.Handle(context.Background(), rabbitmq.OrderCancelledRoutingKey, body, "rid"); outcome != Ack {
		t.Fatalf("got %v, want Ack", outcome)
	}
	if len(notifier.cancelledOrderIDs) != 1 || len(notifier.sentOrderIDs) != 0 {
		t.Fatalf("cancelled=%v confirmed=%v", notifier.cancelledOrderIDs, notifier.sentOrderIDs)
	}
}

func TestUnknownRoutingKeyIsDeadLettered(t *testing.T) {
	handler, _ := newTestHandler()
	if outcome := handler.Handle(context.Background(), "order.exploded", encodedEvent(t, firstEventID), "rid"); outcome != DeadLetter {
		t.Fatalf("got %v, want DeadLetter", outcome)
	}
}
