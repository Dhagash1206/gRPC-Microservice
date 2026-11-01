package outbox

import "context"

// PublishBatch exposes one relay iteration to the external test package.
func (relay *Relay) PublishBatch(ctx context.Context) (int, error) {
	return relay.publishBatch(ctx)
}
