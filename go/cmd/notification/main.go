// Command notification consumes order events from RabbitMQ and emails customers.
//
//	notification              consume
//	notification replay-dlq   move dead-lettered events back into the queue
//
// Env: RABBITMQ_URL, DATABASE_URL, SMTP_ADDR, MAIL_FROM, CUSTOMER_EMAIL_DOMAIN,
// TLS_CERT_FILE, TLS_KEY_FILE, TLS_CA_FILE, OTEL_COLLECTOR_ADDR (telemetry disabled when empty)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"grpcshop/internal/migrate"
	"grpcshop/internal/notification"
	"grpcshop/internal/rabbitmq"
	"grpcshop/internal/telemetry"
	"grpcshop/internal/tlsconfig"
)

func main() {
	logger := slog.New(telemetry.LogHandler{Handler: slog.NewJSONHandler(os.Stdout, nil)}).With("service", "notification")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tlsConfig, err := tlsconfig.ClientConfig(tlsconfig.FilesFromEnv())
	if err != nil {
		fatal(logger, "load TLS config", err)
	}
	if len(os.Args) > 1 && os.Args[1] == "replay-dlq" {
		replayedCount, err := rabbitmq.ReplayDeadLetters(ctx, mustGetenv(logger, "RABBITMQ_URL"), tlsConfig, notification.QueueName)
		if err != nil {
			fatal(logger, "replay dead letters", err)
		}
		logger.Info("replayed dead letters", "count", replayedCount, "queue", notification.QueueName)
		return
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "notification", os.Getenv("OTEL_COLLECTOR_ADDR"), tlsConfig)
	if err != nil {
		fatal(logger, "telemetry", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(flushCtx)
	}()

	poolConfig, err := pgxpool.ParseConfig(mustGetenv(logger, "DATABASE_URL"))
	if err != nil {
		fatal(logger, "DATABASE_URL", err)
	}
	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		fatal(logger, "connect database", err)
	}
	defer pool.Close()
	if err := migrate.Apply(ctx, pool, notification.Migrations(), logger); err != nil {
		fatal(logger, "migrate", err)
	}

	notifier := notification.SMTPNotifier{
		Address:     mustGetenv(logger, "SMTP_ADDR"),
		From:        getenv("MAIL_FROM", "orders@grpcshop.local"),
		EmailDomain: getenv("CUSTOMER_EMAIL_DOMAIN", "customers.grpcshop.local"),
		TLSConfig:   tlsConfig,
		Logger:      logger,
	}
	handler := notification.NewHandler(notifier, notification.NewPostgresProcessedEvents(pool), logger)
	notification.RunConsumer(ctx, mustGetenv(logger, "RABBITMQ_URL"), tlsConfig, handler, logger)
	logger.Info("shut down")
}

func fatal(logger *slog.Logger, message string, err error) {
	logger.Error(message, "error", err)
	os.Exit(1)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func mustGetenv(logger *slog.Logger, key string) string {
	value := os.Getenv(key)
	if value == "" {
		logger.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return value
}
