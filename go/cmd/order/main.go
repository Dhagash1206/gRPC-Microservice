// Command order runs the OrderService gRPC server and the outbox relay.
//
//	order               serve
//	order healthcheck   probe the local server over mTLS (container health check)
//
// Env: PORT, DATABASE_URL, INVENTORY_ADDR, PAYMENT_ADDR, RABBITMQ_URL,
// TLS_CERT_FILE, TLS_KEY_FILE, TLS_CA_FILE, ALLOWED_CALLERS,
// RECOVERY_STALE_AFTER (Go duration, default 60s; must exceed the longest CreateOrder),
// OTEL_COLLECTOR_ADDR (host:port; telemetry disabled when empty)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	inventoryv1 "grpcshop/gen/shop/inventory/v1"
	orderv1 "grpcshop/gen/shop/order/v1"
	paymentv1 "grpcshop/gen/shop/payment/v1"
	"grpcshop/internal/grpcmw"
	"grpcshop/internal/migrate"
	"grpcshop/internal/order"
	"grpcshop/internal/outbox"
	"grpcshop/internal/rabbitmq"
	"grpcshop/internal/telemetry"
	"grpcshop/internal/tlsconfig"
)

const (
	downstreamTimeout = 2 * time.Second
	shutdownGrace     = 10 * time.Second
	recoveryInterval  = 10 * time.Second
	// No legitimate request is anywhere near this; the 4 MiB default only invites abuse.
	maxRequestBytes      = 1 << 20
	maxConcurrentStreams = 256
)

// Transparent retries on UNAVAILABLE. Safe because every downstream mutation is
// idempotent (ReserveStock on order_id, Charge on idempotency_key).
const downstreamServiceConfig = `{
  "methodConfig": [{
    "name": [{}],
    "retryPolicy": {
      "maxAttempts": 4,
      "initialBackoff": "0.1s",
      "maxBackoff": "1s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

func main() {
	logger := slog.New(telemetry.LogHandler{Handler: slog.NewJSONHandler(os.Stdout, nil)}).With("service", "order")
	port := getenv("PORT", "50051")
	tlsFiles := tlsconfig.FilesFromEnv()

	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(port, tlsFiles))
	}
	if err := run(logger, port, tlsFiles); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, port string, tlsFiles tlsconfig.Files) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	telemetryTLSConfig, err := tlsconfig.ClientConfig(tlsFiles)
	if err != nil {
		return err
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "order", os.Getenv("OTEL_COLLECTOR_ADDR"), telemetryTLSConfig)
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(flushCtx)
	}()

	poolConfig, err := pgxpool.ParseConfig(mustGetenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()
	if err := outbox.RegisterBacklogMetrics(pool); err != nil {
		return err
	}
	if err := migrate.Apply(ctx, pool, order.Migrations(), logger); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	clientCredentials, err := tlsconfig.ClientCredentials(tlsFiles)
	if err != nil {
		return err
	}
	inventoryConnection, err := dialService(mustGetenv("INVENTORY_ADDR"), clientCredentials)
	if err != nil {
		return err
	}
	defer inventoryConnection.Close()
	paymentConnection, err := dialService(mustGetenv("PAYMENT_ADDR"), clientCredentials)
	if err != nil {
		return err
	}
	defer paymentConnection.Close()

	amqpTLSConfig, err := tlsconfig.ClientConfig(tlsFiles)
	if err != nil {
		return err
	}
	publisher := rabbitmq.NewPublisher(mustGetenv("RABBITMQ_URL"), amqpTLSConfig, logger)
	defer publisher.Close()
	relayDone := make(chan struct{})
	go func() {
		outbox.NewRelay(pool, publisher, logger).Run(ctx)
		close(relayDone)
	}()

	serverCredentials, err := tlsconfig.ServerCredentials(tlsFiles)
	if err != nil {
		return err
	}
	allowedCallers := strings.Split(getenv("ALLOWED_CALLERS", "envoy"), ",")
	server := grpc.NewServer(
		grpc.Creds(serverCredentials),
		// Health checks run every few seconds; tracing them would drown real traffic.
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.Not(filters.HealthCheck())))),
		grpc.MaxRecvMsgSize(maxRequestBytes),
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		// Envoy's upstream pings are allowed; anything more aggressive is cut off.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.ChainUnaryInterceptor(
			grpcmw.UnaryServerObservability(logger),
			grpcmw.UnaryServerPeerAuthorization(allowedCallers),
		),
	)
	orderService := order.NewService(
		inventoryv1.NewInventoryServiceClient(inventoryConnection),
		paymentv1.NewPaymentServiceClient(paymentConnection),
		order.NewPostgresRepository(pool),
		downstreamTimeout,
		logger,
	)
	orderv1.RegisterOrderServiceServer(server, orderService)

	recoveryStaleAfter, err := time.ParseDuration(getenv("RECOVERY_STALE_AFTER", "60s"))
	if err != nil {
		return fmt.Errorf("RECOVERY_STALE_AFTER: %w", err)
	}
	recoveryDone := make(chan struct{})
	go func() {
		order.NewRecoveryWorker(orderService, recoveryStaleAfter, recoveryInterval, logger).Run(ctx)
		close(recoveryDone)
	}()
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus(orderv1.OrderService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	reflection.Register(server)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	logger.Info("listening (mTLS)", "port", port, "allowed_callers", allowedCallers)

	select {
	case err := <-serveErrors:
		return err
	case <-ctx.Done():
	}

	logger.Info("draining")
	// Report NOT_SERVING first so callers stop routing new calls here.
	healthServer.Shutdown()
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		server.Stop()
	}
	<-relayDone
	<-recoveryDone
	return nil
}

func dialService(target string, transportCredentials credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(downstreamServiceConfig),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.WithChainUnaryInterceptor(grpcmw.UnaryClientRequestIDPropagation()),
	)
}

func runHealthcheck(port string, tlsFiles tlsconfig.Files) int {
	transportCredentials, err := tlsconfig.ClientCredentials(tlsFiles)
	if err != nil {
		return 1
	}
	connection, err := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return 1
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return 1
	}
	return 0
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func mustGetenv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		fmt.Fprintf(os.Stderr, "missing required env var %s\n", key)
		os.Exit(1)
	}
	return value
}
