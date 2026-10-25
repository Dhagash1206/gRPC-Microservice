// Package telemetry configures OpenTelemetry traces and metrics, exported over
// mTLS OTLP/gRPC to the collector. With no endpoint configured it installs
// no-op providers, so tests and local runs need no collector.
package telemetry

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
)

// Setup installs global tracer/meter providers and the W3C propagator. The
// returned function flushes and stops them.
func Setup(ctx context.Context, serviceName, collectorEndpoint string, tlsConfig *tls.Config) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if collectorEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	// Schemaless: a schema URL here must equal resource.Default()'s, which changes
	// whenever the SDK is upgraded, and a mismatch makes Merge fail at startup.
	serviceResource, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}
	transportCredentials := credentials.NewTLS(tlsConfig)

	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(collectorEndpoint), otlptracegrpc.WithTLSCredentials(transportCredentials))
	if err != nil {
		return nil, err
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(serviceResource),
		// Respect the edge's sampling decision; sample everything that starts here.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tracerProvider)

	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(collectorEndpoint), otlpmetricgrpc.WithTLSCredentials(transportCredentials))
	if err != nil {
		return nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(serviceResource),
	)
	otel.SetMeterProvider(meterProvider)

	return func(shutdownCtx context.Context) error {
		return errors.Join(tracerProvider.Shutdown(shutdownCtx), meterProvider.Shutdown(shutdownCtx))
	}, nil
}

// LogHandler adds trace_id/span_id to every record logged with a span in its
// context, so logs can be joined to traces.
type LogHandler struct {
	slog.Handler
}

func (handler LogHandler) Handle(ctx context.Context, record slog.Record) error {
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		record.AddAttrs(slog.String("trace_id", spanContext.TraceID().String()), slog.String("span_id", spanContext.SpanID().String()))
	}
	return handler.Handler.Handle(ctx, record)
}

func (handler LogHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	return LogHandler{handler.Handler.WithAttrs(attributes)}
}

func (handler LogHandler) WithGroup(name string) slog.Handler {
	return LogHandler{handler.Handler.WithGroup(name)}
}

// MapCarrier adapts a string map (e.g. AMQP headers or a stored outbox column)
// for context propagation.
type MapCarrier map[string]string

func (carrier MapCarrier) Get(key string) string { return carrier[key] }
func (carrier MapCarrier) Set(key, value string) { carrier[key] = value }
func (carrier MapCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier))
	for key := range carrier {
		keys = append(keys, key)
	}
	return keys
}
