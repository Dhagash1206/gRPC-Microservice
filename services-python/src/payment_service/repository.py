"""Postgres persistence for payments.

Every outcome is stored under its idempotency key, so retries get the same answer:
succeeded | declined | refunded (cancelled after success) | cancelled (fence: cancelled before any charge).
"""

import uuid
from dataclasses import dataclass

from psycopg_pool import ConnectionPool

STATUS_SUCCEEDED = "succeeded"
STATUS_DECLINED = "declined"
STATUS_REFUNDED = "refunded"
STATUS_CANCELLED = "cancelled"

PAYMENT_COLUMNS = "id, idempotency_key, customer_id, amount_cents, status, decline_reason"


@dataclass(frozen=True)
class PaymentRecord:
    id: str
    idempotency_key: str
    customer_id: str | None
    amount_cents: int | None
    status: str
    decline_reason: str | None


def new_payment_id() -> str:
    return f"pay_{uuid.uuid4().hex[:16]}"


class PaymentRepository:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def record_once(
        self, idempotency_key: str, customer_id: str, amount_cents: int, status: str, decline_reason: str | None
    ) -> PaymentRecord:
        """Inserts the payment unless the key exists; returns whichever record is stored."""
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                f"INSERT INTO payments ({PAYMENT_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s) "
                f"ON CONFLICT (idempotency_key) DO NOTHING RETURNING {PAYMENT_COLUMNS}",
                (new_payment_id(), idempotency_key, customer_id, amount_cents, status, decline_reason),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    f"SELECT {PAYMENT_COLUMNS} FROM payments WHERE idempotency_key = %s", (idempotency_key,)
                ).fetchone()
        return PaymentRecord(*row)

    def cancel(self, idempotency_key: str) -> str:
        """Returns the resulting status: cancelled (fenced), refunded, or declined."""
        with self._pool.connection() as conn, conn.transaction():
            fence_row = conn.execute(
                "INSERT INTO payments (id, idempotency_key, status) VALUES (%s, %s, %s) "
                "ON CONFLICT (idempotency_key) DO NOTHING RETURNING status",
                (new_payment_id(), idempotency_key, STATUS_CANCELLED),
            ).fetchone()
            if fence_row is not None:
                return STATUS_CANCELLED

            (status,) = conn.execute(
                "SELECT status FROM payments WHERE idempotency_key = %s FOR UPDATE", (idempotency_key,)
            ).fetchone()
            if status == STATUS_SUCCEEDED:
                # A real processor's refund API call would go here, keyed by payment id.
                conn.execute(
                    "UPDATE payments SET status = %s, refunded_at = now() WHERE idempotency_key = %s",
                    (STATUS_REFUNDED, idempotency_key),
                )
                return STATUS_REFUNDED
            return status
