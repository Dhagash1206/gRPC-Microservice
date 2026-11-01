package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpPublishing copies a delivered message so it can be republished unchanged
// (Type still carries the event type the consumer dispatches on).
func amqpPublishing(delivery amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:      delivery.Headers,
		ContentType:  delivery.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    delivery.MessageId,
		Type:         delivery.Type,
		Timestamp:    delivery.Timestamp,
		Body:         delivery.Body,
	}
}
