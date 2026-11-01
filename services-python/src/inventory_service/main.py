"""python -m inventory_service.main

Env: PORT, DATABASE_URL, DATABASE_TLS (default true), TLS_CERT_FILE, TLS_KEY_FILE, TLS_CA_FILE, ALLOWED_CALLERS, OTEL_COLLECTOR_ADDR,
     RESERVATION_TTL_SECONDS (default 900), EXPIRY_SWEEP_INTERVAL_SECONDS (default 30)
"""

import logging
import os
import pathlib
import threading
from collections.abc import Collection
from datetime import timedelta

import grpc
from opentelemetry import metrics
from psycopg_pool import ConnectionPool

from common.db import apply_migrations, create_pool
from common.server import ServerHandle, build_server, configure_logging, parse_allowed_callers, run_until_terminated
from common.telemetry import setup_telemetry
from common.tls import TlsFiles
from inventory_service.repository import InventoryRepository
from inventory_service.servicer import InventoryServicer
from shop.inventory.v1 import inventory_pb2, inventory_pb2_grpc

logger = logging.getLogger(__name__)

SERVICE_NAME = "inventory"
FULL_SERVICE_NAME = inventory_pb2.DESCRIPTOR.services_by_name["InventoryService"].full_name
MIGRATIONS_DIR = pathlib.Path(__file__).parent / "migrations"
# envoy: public GET /v1/products. order: reservations.
DEFAULT_ALLOWED_CALLERS = "order,envoy"


def build_inventory_server(
    port: int,
    database_url: str,
    tls_files: TlsFiles,
    allowed_callers: Collection[str],
    reservation_ttl: timedelta = timedelta(minutes=15),
    database_tls_files: TlsFiles | None = None,
) -> tuple[ServerHandle, ConnectionPool, InventoryRepository]:
    pool = create_pool(database_url, database_tls_files)
    apply_migrations(pool, MIGRATIONS_DIR)
    repository = InventoryRepository(pool, reservation_ttl)
    servicer = InventoryServicer(repository)

    def register_servicers(server: grpc.Server) -> None:
        inventory_pb2_grpc.add_InventoryServiceServicer_to_server(servicer, server)

    handle = build_server(SERVICE_NAME, port, register_servicers, [FULL_SERVICE_NAME], tls_files, allowed_callers)
    return handle, pool, repository


def run_expiry_sweeper(repository: InventoryRepository, interval_seconds: float, stop_event: threading.Event) -> None:
    expired_reservations = metrics.get_meter("grpcshop.inventory").create_counter(
        "grpcshop.inventory.expired_reservations", description="Holds released by the TTL sweeper"
    )
    while not stop_event.wait(interval_seconds):
        try:
            released_count = repository.release_expired()
            if released_count:
                expired_reservations.add(released_count)
                logger.warning("released %d expired reservations", released_count)
        except Exception:
            logger.exception("expiry sweep failed")


def main() -> None:
    configure_logging()
    tls_files = TlsFiles.from_env()
    shutdown_telemetry = setup_telemetry(SERVICE_NAME, os.environ.get("OTEL_COLLECTOR_ADDR"), tls_files)
    handle, pool, repository = build_inventory_server(
        port=int(os.environ.get("PORT", "50052")),
        database_url=os.environ["DATABASE_URL"],
        tls_files=tls_files,
        allowed_callers=parse_allowed_callers(DEFAULT_ALLOWED_CALLERS),
        reservation_ttl=timedelta(seconds=int(os.environ.get("RESERVATION_TTL_SECONDS", "900"))),
        database_tls_files=tls_files if os.environ.get("DATABASE_TLS", "true") == "true" else None,
    )
    stop_event = threading.Event()
    sweeper = threading.Thread(
        target=run_expiry_sweeper,
        args=(repository, float(os.environ.get("EXPIRY_SWEEP_INTERVAL_SECONDS", "30")), stop_event),
        daemon=True,
    )
    sweeper.start()
    try:
        run_until_terminated(SERVICE_NAME, handle)
    finally:
        stop_event.set()
        sweeper.join(timeout=5)
        pool.close()
        shutdown_telemetry()


if __name__ == "__main__":
    main()
