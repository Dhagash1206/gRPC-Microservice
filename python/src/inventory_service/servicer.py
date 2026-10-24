import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator

import grpc
from opentelemetry import metrics

from common.identity import forwarded_claims, has_role
from common.interceptors import peer_common_names
from inventory_service.repository import (
    InsufficientStockError,
    InventoryRepository,
    ProductRecord,
    ReservationNotCommittableError,
    ReservationReleasedError,
    RestockLimitError,
    UnknownSkuError,
)
from shop.inventory.v1 import inventory_pb2, inventory_pb2_grpc

logger = logging.getLogger(__name__)

meter = metrics.get_meter("grpcshop.inventory")
reservation_results = meter.create_counter(
    "grpcshop.inventory.reservations", description="ReserveStock results: reserved, insufficient_stock, unknown_sku, fenced"
)
restocked_units = meter.create_counter("grpcshop.inventory.restocked_units", description="Units added by admins")

MAX_SKUS_PER_REQUEST = 100
MAX_QUANTITY_PER_SKU = 10_000
MAX_RESTOCK_QUANTITY = 100_000
VALID_SKU = re.compile(r"[A-Za-z0-9._-]{1,64}")
# Only the edge forwards verified end-user claims; other services must not restock.
RESTOCK_CALLER = "envoy"


def to_product_message(record: ProductRecord) -> inventory_pb2.Product:
    return inventory_pb2.Product(
        sku=record.sku,
        name=record.name,
        unit_price_cents=record.unit_price_cents,
        available_quantity=record.available_quantity,
    )


def require_valid_skus(skus: Iterable[str], context: grpc.ServicerContext) -> None:
    # Postgres rejects NUL bytes with an internal error, and bad SKUs must not be echoed back.
    if not all(VALID_SKU.fullmatch(sku) for sku in skus):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "sku must be 1-64 characters of [A-Za-z0-9._-]")


def require_order_id(order_id: str, context: grpc.ServicerContext) -> None:
    if not order_id:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "order_id is required")


class InventoryServicer(inventory_pb2_grpc.InventoryServiceServicer):
    def __init__(self, repository: InventoryRepository) -> None:
        self._repository = repository

    def BatchGetProducts(self, request, context):
        if len(request.skus) > MAX_SKUS_PER_REQUEST:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"at most {MAX_SKUS_PER_REQUEST} SKUs per request")
        require_valid_skus(request.skus, context)
        try:
            records = self._repository.get_products(list(request.skus))
        except UnknownSkuError as error:
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        return inventory_pb2.BatchGetProductsResponse(products=[to_product_message(record) for record in records])

    def ListProducts(self, request, context) -> Iterator[inventory_pb2.Product]:
        for record in self._repository.list_products():
            yield to_product_message(record)

    def ReserveStock(self, request, context):
        require_order_id(request.order_id, context)
        if not request.items:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "items must not be empty")
        if len(request.items) > MAX_SKUS_PER_REQUEST:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"at most {MAX_SKUS_PER_REQUEST} items per request")
        require_valid_skus((item.sku for item in request.items), context)

        quantities_by_sku: dict[str, int] = defaultdict(int)
        for item in request.items:
            if item.quantity <= 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"quantity for {item.sku} must be positive")
            quantities_by_sku[item.sku] += item.quantity
            if quantities_by_sku[item.sku] > MAX_QUANTITY_PER_SKU:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"quantity for {item.sku} exceeds {MAX_QUANTITY_PER_SKU}")

        try:
            reservation_id = self._repository.reserve(request.order_id, dict(quantities_by_sku))
        except UnknownSkuError as error:
            reservation_results.add(1, {"result": "unknown_sku"})
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        except InsufficientStockError as error:
            reservation_results.add(1, {"result": "insufficient_stock"})
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        except ReservationReleasedError as error:
            reservation_results.add(1, {"result": "fenced"})
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        reservation_results.add(1, {"result": "reserved"})

        logger.info("reserved %s for order %s", reservation_id, request.order_id)
        return inventory_pb2.ReserveStockResponse(reservation_id=reservation_id)

    def CommitReservation(self, request, context):
        require_order_id(request.order_id, context)
        try:
            self._repository.commit(request.order_id)
        except ReservationNotCommittableError as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        return inventory_pb2.CommitReservationResponse()

    def ReleaseStock(self, request, context):
        require_order_id(request.order_id, context)
        if self._repository.release(request.order_id):
            logger.info("released stock for order %s", request.order_id)
        else:
            logger.info("nothing held for order %s; fence recorded", request.order_id)
        return inventory_pb2.ReleaseStockResponse()

    def RestockProduct(self, request, context):
        # Envoy's RBAC already requires the admin role; re-checking here means a
        # compromised internal caller cannot restock either.
        if RESTOCK_CALLER not in peer_common_names(context) or not has_role(forwarded_claims(context), "admin"):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "restocking requires the admin role")
        if not 1 <= request.quantity <= MAX_RESTOCK_QUANTITY:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"quantity must be between 1 and {MAX_RESTOCK_QUANTITY}")
        require_valid_skus([request.sku], context)
        try:
            record = self._repository.restock(request.sku, request.quantity)
        except UnknownSkuError as error:
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        except RestockLimitError as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        restocked_units.add(request.quantity, {"sku": record.sku})
        logger.info("restocked %s by %d (now %d)", record.sku, request.quantity, record.available_quantity)
        return to_product_message(record)
