"""OpenTelemetry setup: traces and metrics exported over mTLS OTLP/gRPC.

Must run before the gRPC server and database pool are created, because the
instrumentations patch grpc.server and psycopg connections. Without a collector
address it does nothing, so tests run uninstrumented.
"""

import logging
from collections.abc import Callable

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer, filters
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from common.tls import TlsFiles, channel_credentials


class TraceContextLogFilter(logging.Filter):
    """Adds trace_id to log records so log lines can be joined to traces."""

    def filter(self, record: logging.LogRecord) -> bool:
        span_context = trace.get_current_span().get_span_context()
        record.trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else "-"
        return True


def setup_telemetry(service_name: str, collector_address: str | None, tls_files: TlsFiles) -> Callable[[], None]:
    if not collector_address:
        return lambda: None

    resource = Resource.create({SERVICE_NAME: service_name})
    credentials = channel_credentials(tls_files)

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_address, credentials=credentials))
    )
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=collector_address, credentials=credentials), export_interval_millis=10_000
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    # Health checks run every few seconds; tracing them would drown real traffic.
    GrpcInstrumentorServer(filter_=filters.negate(filters.health_check())).instrument()
    PsycopgInstrumentor().instrument(enable_commenter=False)

    def shutdown() -> None:
        tracer_provider.shutdown()
        meter_provider.shutdown()

    return shutdown
