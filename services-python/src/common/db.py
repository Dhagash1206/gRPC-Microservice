"""Postgres connection pool and a minimal forward-only migration runner."""

import logging
import os
import pathlib
import tempfile

from psycopg_pool import ConnectionPool

from common.tls import TlsFiles

logger = logging.getLogger(__name__)

# Arbitrary constant; serializes migrations when several replicas start at once.
MIGRATION_ADVISORY_LOCK_ID = 727_001


def _libpq_private_key_copy(key_file: pathlib.Path) -> str:
    # libpq refuses keys readable by group/others, and mounted secrets often are,
    # so hand it a private 0600 copy.
    file_descriptor, copy_path = tempfile.mkstemp(prefix="pgclient-", suffix=".key")
    with os.fdopen(file_descriptor, "wb") as key_copy:
        key_copy.write(key_file.read_bytes())
    os.chmod(copy_path, 0o600)
    return copy_path


def create_pool(database_url: str, tls_files: TlsFiles | None = None, max_size: int = 10) -> ConnectionPool:
    """With tls_files: TLS with full server verification, authenticated by client certificate."""
    connection_kwargs: dict = {"autocommit": True}
    if tls_files is not None:
        connection_kwargs.update(
            sslmode="verify-full",
            sslrootcert=str(tls_files.ca_file),
            sslcert=str(tls_files.cert_file),
            sslkey=_libpq_private_key_copy(tls_files.key_file),
        )
    # autocommit=True: every write path opens an explicit `conn.transaction()`.
    pool = ConnectionPool(database_url, min_size=1, max_size=max_size, kwargs=connection_kwargs, open=False)
    pool.open(wait=True, timeout=30)
    return pool


def apply_migrations(pool: ConnectionPool, migrations_dir: pathlib.Path) -> None:
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_ADVISORY_LOCK_ID,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            applied_versions = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for migration_path in sorted(migrations_dir.glob("*.sql")):
                if migration_path.name in applied_versions:
                    continue
                with conn.transaction():
                    conn.execute(migration_path.read_text())
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (migration_path.name,))
                logger.info("applied migration %s", migration_path.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_ADVISORY_LOCK_ID,))
