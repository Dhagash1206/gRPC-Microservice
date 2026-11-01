package order

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	tracer = otel.Tracer("grpcshop/order")
	meter  = otel.Meter("grpcshop/order")

	checkoutOutcomes, _ = meter.Int64Counter("grpcshop.checkout.outcomes",
		metric.WithDescription("CreateOrder results: confirmed, rejected (business rule), failed, aborted, replayed"))
	compensations, _ = meter.Int64Counter("grpcshop.saga.compensations",
		metric.WithDescription("Compensation attempts by trigger and result"))
	recoveredOrders, _ = meter.Int64Counter("grpcshop.saga.recovered",
		metric.WithDescription("Abandoned orders finished by the recovery worker"))
)

func recordCheckout(ctx context.Context, outcome string) {
	checkoutOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
