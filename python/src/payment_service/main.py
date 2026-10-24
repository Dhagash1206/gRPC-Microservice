"""python -m payment_service.main

Env: PORT, DATABASE_URL, DATABASE_TLS, MAX_CHARGE_CENTS, TLS_CERT_FILE, TLS_KEY_FILE, TLS_CA_FILE,
     ALLOWED_CALLERS, OTEL_COLLECTOR_ADDR
"""

import os
import pathlib
from collections.abc import Collection

import grpc
from psycopg_pool import ConnectionPool

from common.db import apply_migrations, create_pool
from common.server import ServerHandle, build_server, configure_logging, parse_allowed_callers, run_until_terminated
from common.telemetry import setup_telemetry
from common.tls import TlsFiles
from payment_service.repository import PaymentRepository
from payment_service.servicer import PaymentServicer
from shop.payment.v1 import payment_pb2, payment_pb2_grpc

SERVICE_NAME = "payment"
FULL_SERVICE_NAME = payment_pb2.DESCRIPTOR.services_by_name["PaymentService"].full_name
MIGRATIONS_DIR = pathlib.Path(__file__).parent / "migrations"
DEFAULT_ALLOWED_CALLERS = "order"
DEFAULT_MAX_CHARGE_CENTS = 100_000


def build_payment_server(
    port: int,
    database_url: str,
    tls_files: TlsFiles,
    allowed_callers: Collection[str],
    max_charge_cents: int = DEFAULT_MAX_CHARGE_CENTS,
    database_tls_files: TlsFiles | None = None,
) -> tuple[ServerHandle, ConnectionPool]:
    pool = create_pool(database_url, database_tls_files)
    apply_migrations(pool, MIGRATIONS_DIR)
    servicer = PaymentServicer(PaymentRepository(pool), max_charge_cents)

    def register_servicers(server: grpc.Server) -> None:
        payment_pb2_grpc.add_PaymentServiceServicer_to_server(servicer, server)

    handle = build_server(SERVICE_NAME, port, register_servicers, [FULL_SERVICE_NAME], tls_files, allowed_callers)
    return handle, pool


def main() -> None:
    configure_logging()
    tls_files = TlsFiles.from_env()
    shutdown_telemetry = setup_telemetry(SERVICE_NAME, os.environ.get("OTEL_COLLECTOR_ADDR"), tls_files)
    handle, pool = build_payment_server(
        port=int(os.environ.get("PORT", "50053")),
        database_url=os.environ["DATABASE_URL"],
        tls_files=tls_files,
        allowed_callers=parse_allowed_callers(DEFAULT_ALLOWED_CALLERS),
        max_charge_cents=int(os.environ.get("MAX_CHARGE_CENTS", DEFAULT_MAX_CHARGE_CENTS)),
        database_tls_files=tls_files if os.environ.get("DATABASE_TLS", "true") == "true" else None,
    )
    try:
        run_until_terminated(SERVICE_NAME, handle)
    finally:
        pool.close()
        shutdown_telemetry()


if __name__ == "__main__":
    main()
