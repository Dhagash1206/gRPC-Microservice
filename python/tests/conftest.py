"""Fixtures: a dev CA, one Postgres container, and per-test databases cloned from migrated templates."""

import pathlib
import subprocess
import uuid
from dataclasses import dataclass
from datetime import timedelta

import grpc
import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from common.db import apply_migrations, create_pool
from common.server import ServerHandle
from common.tls import TlsFiles, channel_credentials
from inventory_service import main as inventory_main
from payment_service import main as payment_main
from shop.inventory.v1 import inventory_pb2_grpc
from shop.payment.v1 import payment_pb2_grpc

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]


@dataclass
class RunningService:
    handle: ServerHandle
    certs_dir: pathlib.Path
    repository: object = None

    def channel(self, workload_name: str) -> grpc.Channel:
        """mTLS channel presenting workload_name's client certificate."""
        credentials = channel_credentials(TlsFiles.for_workload(self.certs_dir, workload_name))
        return grpc.secure_channel(f"localhost:{self.handle.port}", credentials)


@pytest.fixture(scope="session")
def certs_dir(tmp_path_factory) -> pathlib.Path:
    output_dir = tmp_path_factory.mktemp("certs")
    subprocess.run([PROJECT_ROOT / "scripts/gen_certs.sh", output_dir], check=True, capture_output=True)
    return output_dir


@pytest.fixture(scope="session")
def postgres_admin_url():
    with PostgresContainer("postgres:17-alpine", driver=None) as postgres:
        yield postgres.get_connection_url()


def _database_url(admin_url: str, database_name: str) -> str:
    return admin_url.rsplit("/", 1)[0] + f"/{database_name}"


@pytest.fixture(scope="session")
def create_database(postgres_admin_url):
    """Returns create(migrations_dir) -> URL of a fresh, fully migrated database."""
    template_names_by_dir: dict[pathlib.Path, str] = {}

    def create(migrations_dir: pathlib.Path) -> str:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin_conn:
            if migrations_dir not in template_names_by_dir:
                template_name = f"template_{migrations_dir.parent.name}"
                admin_conn.execute(f"CREATE DATABASE {template_name}")
                with create_pool(_database_url(postgres_admin_url, template_name)) as template_pool:
                    apply_migrations(template_pool, migrations_dir)
                template_names_by_dir[migrations_dir] = template_name

            database_name = f"test_{uuid.uuid4().hex[:12]}"
            admin_conn.execute(f"CREATE DATABASE {database_name} TEMPLATE {template_names_by_dir[migrations_dir]}")
        return _database_url(postgres_admin_url, database_name)

    return create


@pytest.fixture
def reservation_ttl() -> timedelta:
    """Override in a test module to exercise expiry."""
    return timedelta(minutes=15)


@pytest.fixture
def inventory_service(certs_dir, create_database, reservation_ttl):
    handle, pool, repository = inventory_main.build_inventory_server(
        port=0,
        database_url=create_database(inventory_main.MIGRATIONS_DIR),
        tls_files=TlsFiles.for_workload(certs_dir, "inventory"),
        allowed_callers=["order", "envoy"],
        reservation_ttl=reservation_ttl,
    )
    handle.server.start()
    yield RunningService(handle, certs_dir, repository)
    handle.server.stop(grace=None)
    pool.close()


@pytest.fixture
def payment_service(certs_dir, create_database):
    handle, pool = payment_main.build_payment_server(
        port=0,
        database_url=create_database(payment_main.MIGRATIONS_DIR),
        tls_files=TlsFiles.for_workload(certs_dir, "payment"),
        allowed_callers=["order"],
        max_charge_cents=100_000,
    )
    handle.server.start()
    yield RunningService(handle, certs_dir)
    handle.server.stop(grace=None)
    pool.close()


@pytest.fixture
def inventory_stub(inventory_service):
    with inventory_service.channel("order") as channel:
        yield inventory_pb2_grpc.InventoryServiceStub(channel)


@pytest.fixture
def payment_stub(payment_service):
    with payment_service.channel("order") as channel:
        yield payment_pb2_grpc.PaymentServiceStub(channel)
