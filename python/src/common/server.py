"""Server bootstrap shared by the Python services."""

import logging
import os
import signal
from collections.abc import Callable, Collection, Sequence
from concurrent import futures
from dataclasses import dataclass

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from common.interceptors import PeerAuthorizationInterceptor, ServerObservabilityInterceptor
from common.telemetry import TraceContextLogFilter
from common.tls import TlsFiles, server_credentials

logger = logging.getLogger(__name__)

SHUTDOWN_GRACE_SECONDS = 10

SERVER_OPTIONS = [
    # Accept the Go client's keepalive pings (every 30s) instead of answering with GOAWAY.
    ("grpc.http2.min_recv_ping_interval_without_data_ms", 20_000),
    # No legitimate request is anywhere near this; the 4 MiB default only invites abuse.
    ("grpc.max_receive_message_length", 1024 * 1024),
]
# Beyond this many in-flight RPCs, new calls fail fast with RESOURCE_EXHAUSTED instead
# of queueing without bound behind the thread pool.
MAXIMUM_CONCURRENT_RPCS = 128


@dataclass
class ServerHandle:
    server: grpc.Server
    port: int
    health_servicer: health.HealthServicer


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s: %(message)s"
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(TraceContextLogFilter())


def parse_allowed_callers(default: str) -> list[str]:
    return [name.strip() for name in os.environ.get("ALLOWED_CALLERS", default).split(",") if name.strip()]


def build_server(
    service_name: str,
    port: int,
    register_servicers: Callable[[grpc.Server], None],
    full_service_names: Sequence[str],
    tls_files: TlsFiles,
    allowed_callers: Collection[str],
    max_workers: int = 16,
) -> ServerHandle:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        # Observability is outermost so rejected calls are logged too.
        interceptors=[ServerObservabilityInterceptor(service_name), PeerAuthorizationInterceptor(allowed_callers)],
        options=SERVER_OPTIONS,
        maximum_concurrent_rpcs=MAXIMUM_CONCURRENT_RPCS,
    )
    register_servicers(server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    for health_service_name in ("", *full_service_names):
        health_servicer.set(health_service_name, health_pb2.HealthCheckResponse.SERVING)

    reflection.enable_server_reflection((*full_service_names, health.SERVICE_NAME, reflection.SERVICE_NAME), server)

    bound_port = server.add_secure_port(f"[::]:{port}", server_credentials(tls_files))
    return ServerHandle(server, bound_port, health_servicer)


def run_until_terminated(service_name: str, handle: ServerHandle) -> None:
    handle.server.start()
    logger.info("%s listening on port %d (mTLS)", service_name, handle.port)

    def handle_shutdown(signal_number, _frame):
        logger.info("%s received %s, draining", service_name, signal.Signals(signal_number).name)
        # Report NOT_SERVING first so callers stop routing new calls here.
        handle.health_servicer.enter_graceful_shutdown()
        handle.server.stop(grace=SHUTDOWN_GRACE_SECONDS)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    handle.server.wait_for_termination()
