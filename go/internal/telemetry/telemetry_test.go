package telemetry

import (
	"context"
	"crypto/tls"
	"testing"
	"time"
)

// Exporters connect lazily, so Setup must succeed without a reachable collector;
// this catches startup failures such as resource schema conflicts.
func TestSetupWithCollectorEndpoint(t *testing.T) {
	shutdown, err := Setup(context.Background(), "test-service", "localhost:1", &tls.Config{MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = shutdown(ctx) // flushing to an unreachable collector may time out; only Setup matters here
}

func TestSetupWithoutEndpointIsNoop(t *testing.T) {
	shutdown, err := Setup(context.Background(), "test-service", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
