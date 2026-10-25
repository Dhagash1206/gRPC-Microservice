package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
)

// ReplayDeadLetters moves the messages currently in queueName's dead-letter queue
// back into queueName. Each message is acked from the DLQ only after the broker
// confirms the republish, so a failure part-way loses nothing.
func ReplayDeadLetters(ctx context.Context, url string, tlsConfig *tls.Config, queueName string) (int, error) {
	connection, err := Dial(url, tlsConfig)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return 0, err
	}
	if err := channel.Confirm(false); err != nil {
		return 0, err
	}
	deadLetterQueue, err := channel.QueueDeclarePassive(DeadLetterQueue(queueName), true, false, false, false, nil)
	if err != nil {
		return 0, err
	}

	// Bounded by the starting depth so messages that fail again are not looped forever.
	replayedCount := 0
	for range deadLetterQueue.Messages {
		delivery, found, err := channel.Get(DeadLetterQueue(queueName), false)
		if err != nil {
			return replayedCount, err
		}
		if !found {
			break
		}
		confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, RetryExchange(queueName), queueName, true, false,
			amqpPublishing(delivery))
		if err != nil {
			return replayedCount, err
		}
		acknowledged, err := confirmation.WaitContext(ctx)
		if err != nil || !acknowledged {
			_ = delivery.Nack(false, true)
			return replayedCount, fmt.Errorf("republish not confirmed: %v", err)
		}
		if err := delivery.Ack(false); err != nil {
			return replayedCount, err
		}
		replayedCount++
	}
	return replayedCount, nil
}
