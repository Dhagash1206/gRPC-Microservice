// Package rabbitmq declares the event topology and provides a confirming publisher.
//
//	shop.events (topic) --order.*--> <consumer queue> (quorum) <-- <consumer queue>.retry (direct, DLQ replay)
//	                                              | rejected or > deliveryLimit redeliveries
//	                                              v
//	shop.events.dlx (direct) ---------------> <consumer queue>.dlq
package rabbitmq

import (
	"crypto/tls"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventsExchange           = "shop.events"
	DeadLetterExchange       = "shop.events.dlx"
	OrderConfirmedRoutingKey = "order.confirmed"
	OrderCancelledRoutingKey = "order.cancelled"
	deliveryLimit            = 5
)

func DeclareEventsExchange(channel *amqp.Channel) error {
	return channel.ExchangeDeclare(EventsExchange, amqp.ExchangeTopic, true, false, false, false, nil)
}

// DeclareConsumerQueue declares a durable quorum queue bound to routingKeys, plus its dead-letter queue.
func DeclareConsumerQueue(channel *amqp.Channel, queueName string, routingKeys ...string) error {
	if err := DeclareEventsExchange(channel); err != nil {
		return err
	}
	if err := channel.ExchangeDeclare(DeadLetterExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return err
	}
	deadLetterQueueName := DeadLetterQueue(queueName)
	if _, err := channel.QueueDeclare(deadLetterQueueName, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return err
	}
	if err := channel.QueueBind(deadLetterQueueName, queueName, DeadLetterExchange, false, nil); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-delivery-limit":          deliveryLimit,
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": queueName,
	}); err != nil {
		return err
	}
	for _, routingKey := range routingKeys {
		if err := channel.QueueBind(queueName, routingKey, EventsExchange, false, nil); err != nil {
			return err
		}
	}
	// A per-queue replay exchange lets the consumer re-inject dead letters into its own
	// queue only, without permission to publish to shop.events or the default exchange.
	if err := channel.ExchangeDeclare(RetryExchange(queueName), amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return err
	}
	return channel.QueueBind(queueName, queueName, RetryExchange(queueName), false, nil)
}

func RetryExchange(queueName string) string {
	return queueName + ".retry"
}

func DeadLetterQueue(queueName string) string {
	return queueName + ".dlq"
}

// Dial connects with TLS and authenticates with the client certificate (SASL
// EXTERNAL), so no broker password exists. A nil tlsConfig dials plaintext (tests).
func Dial(url string, tlsConfig *tls.Config) (*amqp.Connection, error) {
	if tlsConfig == nil {
		return amqp.Dial(url)
	}
	return amqp.DialTLS_ExternalAuth(url, tlsConfig.Clone())
}
