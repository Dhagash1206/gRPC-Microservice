package rabbitmq

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"grpcshop/internal/outbox"
)

// Publisher publishes outbox messages with publisher confirms and the mandatory
// flag, so a message only counts as published once the broker has stored it in
// at least one queue. It reconnects lazily after any channel failure.
type Publisher struct {
	url       string
	tlsConfig *tls.Config
	logger    *slog.Logger

	mutex      sync.Mutex
	connection *amqp.Connection
	channel    *amqp.Channel
	returns    chan amqp.Return
}

func NewPublisher(url string, tlsConfig *tls.Config, logger *slog.Logger) *Publisher {
	return &Publisher{url: url, tlsConfig: tlsConfig, logger: logger}
}

func (publisher *Publisher) Close() {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()
	publisher.resetLocked()
}

func (publisher *Publisher) Publish(ctx context.Context, messages []outbox.Message) ([]string, error) {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()

	if err := publisher.ensureChannelLocked(); err != nil {
		return nil, err
	}

	confirmations := make([]*amqp.DeferredConfirmation, len(messages))
	var publishErr error
	for index, message := range messages {
		confirmations[index], publishErr = publisher.channel.PublishWithDeferredConfirmWithContext(
			ctx, EventsExchange, message.RoutingKey, true, false, amqp.Publishing{
				ContentType:  "application/x-protobuf",
				DeliveryMode: amqp.Persistent,
				MessageId:    message.ID,
				Type:         message.RoutingKey,
				Timestamp:    time.Now().UTC(),
				Headers:      messageHeaders(message),
				Body:         message.Payload,
			})
		if publishErr != nil {
			break
		}
	}

	var acknowledgedIDs []string
	for index, confirmation := range confirmations {
		if confirmation == nil {
			continue
		}
		acknowledged, err := confirmation.WaitContext(ctx)
		if err != nil {
			publishErr = err
			break
		}
		if acknowledged {
			acknowledgedIDs = append(acknowledgedIDs, messages[index].ID)
		}
	}

	// basic.return precedes the ack for the same message, so all returns for
	// this batch are already buffered once every confirmation has arrived.
	returnedIDs := map[string]bool{}
	for drained := false; !drained; {
		select {
		case returned := <-publisher.returns:
			returnedIDs[returned.MessageId] = true
			publisher.logger.Warn("event unroutable, will retry", "event_id", returned.MessageId,
				"routing_key", returned.RoutingKey)
		default:
			drained = true
		}
	}

	publishedIDs := make([]string, 0, len(acknowledgedIDs))
	for _, messageID := range acknowledgedIDs {
		if !returnedIDs[messageID] {
			publishedIDs = append(publishedIDs, messageID)
		}
	}

	if publishErr != nil {
		publisher.resetLocked()
	}
	return publishedIDs, publishErr
}

func (publisher *Publisher) ensureChannelLocked() error {
	if publisher.channel != nil && !publisher.channel.IsClosed() {
		return nil
	}
	publisher.resetLocked()

	connection, err := Dial(publisher.url, publisher.tlsConfig)
	if err != nil {
		return err
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return err
	}
	if err := channel.Confirm(false); err != nil {
		connection.Close()
		return err
	}
	if err := DeclareEventsExchange(channel); err != nil {
		connection.Close()
		return err
	}
	publisher.connection = connection
	publisher.channel = channel
	publisher.returns = channel.NotifyReturn(make(chan amqp.Return, 256))
	publisher.logger.Info("connected to rabbitmq")
	return nil
}

func (publisher *Publisher) resetLocked() {
	if publisher.connection != nil {
		publisher.connection.Close() //nolint:errcheck
	}
	publisher.connection, publisher.channel, publisher.returns = nil, nil, nil
}

func messageHeaders(message outbox.Message) amqp.Table {
	headers := amqp.Table{"x-request-id": message.RequestID}
	for key, value := range message.TraceHeaders {
		headers[key] = value
	}
	return headers
}
