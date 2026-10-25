"""Postgres persistence for products and stock reservations.

Reservation lifecycle: held -> committed | released. Held reservations expire
after a TTL so a crashed caller cannot pin stock forever. Releasing an order
that has no reservation yet stores a released "fence" row, so a ReserveStock
that arrives late (e.g. a retry still in flight) cannot take stock afterwards.

Row locks are always taken in SKU order so concurrent calls cannot deadlock,
and CHECK (available_quantity >= 0) is the last line of defence against overselling.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta

from psycopg import Connection
from psycopg_pool import ConnectionPool

STATUS_HELD = "held"
STATUS_COMMITTED = "committed"
STATUS_RELEASED = "released"

MAX_SKUS_IN_ERROR = 10
MAX_AVAILABLE_QUANTITY = 1_000_000


@dataclass(frozen=True)
class ProductRecord:
    sku: str
    name: str
    unit_price_cents: int
    available_quantity: int


class UnknownSkuError(Exception):
    def __init__(self, skus: list[str]) -> None:
        shown = ", ".join(skus[:MAX_SKUS_IN_ERROR])
        more = f" (+{len(skus) - MAX_SKUS_IN_ERROR} more)" if len(skus) > MAX_SKUS_IN_ERROR else ""
        super().__init__(f"unknown SKUs: {shown}{more}")


class InsufficientStockError(Exception):
    def __init__(self, sku: str, requested: int, available: int) -> None:
        super().__init__(f"insufficient stock for {sku}: requested {requested}, available {available}")


class ReservationReleasedError(Exception):
    def __init__(self, order_id: str) -> None:
        super().__init__(f"reservation for order {order_id} was released")


class RestockLimitError(Exception):
    def __init__(self, sku: str) -> None:
        super().__init__(f"restocking {sku} would exceed {MAX_AVAILABLE_QUANTITY} units")


class ReservationNotCommittableError(Exception):
    def __init__(self, order_id: str) -> None:
        super().__init__(f"reservation for order {order_id} is missing, released or expired")


PRODUCT_COLUMNS = "sku, name, unit_price_cents, available_quantity"


class InventoryRepository:
    def __init__(self, pool: ConnectionPool, reservation_ttl: timedelta) -> None:
        self._pool = pool
        self._reservation_ttl = reservation_ttl

    def get_products(self, skus: list[str]) -> list[ProductRecord]:
        unique_skus = list(dict.fromkeys(skus))
        with self._pool.connection() as conn:
            rows = conn.execute(f"SELECT {PRODUCT_COLUMNS} FROM products WHERE sku = ANY(%s)", (unique_skus,)).fetchall()
        records_by_sku = {row[0]: ProductRecord(*row) for row in rows}
        missing_skus = [sku for sku in unique_skus if sku not in records_by_sku]
        if missing_skus:
            raise UnknownSkuError(missing_skus)
        return [records_by_sku[sku] for sku in unique_skus]

    def list_products(self) -> list[ProductRecord]:
        with self._pool.connection() as conn:
            rows = conn.execute(f"SELECT {PRODUCT_COLUMNS} FROM products ORDER BY sku").fetchall()
        return [ProductRecord(*row) for row in rows]

    def restock(self, sku: str, quantity: int) -> ProductRecord:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                f"UPDATE products SET available_quantity = available_quantity + %s WHERE sku = %s "
                f"AND available_quantity + %s <= %s RETURNING {PRODUCT_COLUMNS}",
                (quantity, sku, quantity, MAX_AVAILABLE_QUANTITY),
            ).fetchone()
            if row is not None:
                return ProductRecord(*row)
            exists = conn.execute("SELECT 1 FROM products WHERE sku = %s", (sku,)).fetchone()
        if exists is None:
            raise UnknownSkuError([sku])
        raise RestockLimitError(sku)

    def reserve(self, order_id: str, quantities_by_sku: dict[str, int]) -> str:
        reservation_id = f"res_{uuid.uuid4().hex[:16]}"
        sorted_skus = sorted(quantities_by_sku)

        with self._pool.connection() as conn, conn.transaction():
            inserted_row = conn.execute(
                "INSERT INTO reservations (id, order_id, status, expires_at) VALUES (%s, %s, %s, now() + %s) "
                "ON CONFLICT (order_id) DO NOTHING RETURNING id",
                (reservation_id, order_id, STATUS_HELD, self._reservation_ttl),
            ).fetchone()
            if inserted_row is None:
                # A retry of an earlier call (or a call after a fence): return the original outcome.
                existing_id, status = conn.execute(
                    "SELECT id, status FROM reservations WHERE order_id = %s", (order_id,)
                ).fetchone()
                if status == STATUS_RELEASED:
                    raise ReservationReleasedError(order_id)
                return existing_id

            available_by_sku = dict(
                conn.execute(
                    "SELECT sku, available_quantity FROM products WHERE sku = ANY(%s) ORDER BY sku FOR UPDATE",
                    (sorted_skus,),
                ).fetchall()
            )
            missing_skus = [sku for sku in sorted_skus if sku not in available_by_sku]
            if missing_skus:
                raise UnknownSkuError(missing_skus)
            for sku in sorted_skus:
                if available_by_sku[sku] < quantities_by_sku[sku]:
                    raise InsufficientStockError(sku, quantities_by_sku[sku], available_by_sku[sku])

            with conn.cursor() as cursor:
                cursor.executemany(
                    "UPDATE products SET available_quantity = available_quantity - %s WHERE sku = %s",
                    [(quantities_by_sku[sku], sku) for sku in sorted_skus],
                )
                cursor.executemany(
                    "INSERT INTO reservation_items (reservation_id, sku, quantity) VALUES (%s, %s, %s)",
                    [(reservation_id, sku, quantities_by_sku[sku]) for sku in sorted_skus],
                )
        return reservation_id

    def commit(self, order_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            committed_row = conn.execute(
                "UPDATE reservations SET status = %s WHERE order_id = %s "
                "AND (status = %s OR (status = %s AND expires_at > now())) RETURNING id",
                (STATUS_COMMITTED, order_id, STATUS_COMMITTED, STATUS_HELD),
            ).fetchone()
        if committed_row is None:
            raise ReservationNotCommittableError(order_id)

    def release(self, order_id: str) -> bool:
        """Returns True if stock was returned; False if nothing was held (a fence is stored)."""
        with self._pool.connection() as conn, conn.transaction():
            fence_row = conn.execute(
                "INSERT INTO reservations (id, order_id, status, released_at, expires_at) "
                "VALUES (%s, %s, %s, now(), now()) ON CONFLICT (order_id) DO NOTHING RETURNING id",
                (f"res_{uuid.uuid4().hex[:16]}", order_id, STATUS_RELEASED),
            ).fetchone()
            if fence_row is not None:
                return False
            released_row = conn.execute(
                "UPDATE reservations SET status = %s, released_at = now() "
                "WHERE order_id = %s AND status <> %s RETURNING id",
                (STATUS_RELEASED, order_id, STATUS_RELEASED),
            ).fetchone()
            if released_row is None:
                return False
            self._restore_stock(conn, released_row[0])
        return True

    def release_expired(self, limit: int = 100) -> int:
        """Releases held reservations past their TTL; safe to run on several replicas."""
        with self._pool.connection() as conn, conn.transaction():
            expired_ids = [
                row[0]
                for row in conn.execute(
                    "UPDATE reservations SET status = %s, released_at = now() WHERE id IN ("
                    " SELECT id FROM reservations WHERE status = %s AND expires_at <= now()"
                    " ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED) RETURNING id",
                    (STATUS_RELEASED, STATUS_HELD, limit),
                ).fetchall()
            ]
            for reservation_id in expired_ids:
                self._restore_stock(conn, reservation_id)
        return len(expired_ids)

    @staticmethod
    def _restore_stock(conn: Connection, reservation_id: str) -> None:
        conn.execute(
            "SELECT sku FROM products WHERE sku IN "
            "(SELECT sku FROM reservation_items WHERE reservation_id = %s) ORDER BY sku FOR UPDATE",
            (reservation_id,),
        )
        conn.execute(
            "UPDATE products AS product SET available_quantity = product.available_quantity + item.quantity "
            "FROM reservation_items AS item WHERE item.reservation_id = %s AND item.sku = product.sku",
            (reservation_id,),
        )
