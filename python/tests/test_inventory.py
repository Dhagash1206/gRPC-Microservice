import base64
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import grpc
import pytest

from shop.inventory.v1 import inventory_pb2, inventory_pb2_grpc


def stock_levels(inventory_stub) -> dict[str, int]:
    return {
        product.sku: product.available_quantity
        for product in inventory_stub.ListProducts(inventory_pb2.ListProductsRequest())
    }


def reserve(inventory_stub, items: dict[str, int], order_id: str | None = None):
    return inventory_stub.ReserveStock(
        inventory_pb2.ReserveStockRequest(
            order_id=order_id or str(uuid.uuid4()),
            items=[inventory_pb2.StockItem(sku=sku, quantity=quantity) for sku, quantity in items.items()],
        )
    )


def release(inventory_stub, order_id: str) -> None:
    inventory_stub.ReleaseStock(inventory_pb2.ReleaseStockRequest(order_id=order_id))


def commit(inventory_stub, order_id: str) -> None:
    inventory_stub.CommitReservation(inventory_pb2.CommitReservationRequest(order_id=order_id))


def assert_rpc_error(code: grpc.StatusCode, call, *args):
    with pytest.raises(grpc.RpcError) as error_info:
        call(*args)
    assert error_info.value.code() == code, error_info.value.details()
    return error_info.value


def test_list_products_streams_seeded_catalog(inventory_stub):
    assert stock_levels(inventory_stub) == {"KEYBOARD": 10, "LAPTOP": 5, "MONITOR": 3, "MOUSE": 25}


def test_batch_get_products_preserves_request_order(inventory_stub):
    response = inventory_stub.BatchGetProducts(inventory_pb2.BatchGetProductsRequest(skus=["MOUSE", "KEYBOARD"]))
    assert [product.sku for product in response.products] == ["MOUSE", "KEYBOARD"]


def test_batch_get_unknown_sku_is_not_found(inventory_stub):
    error = assert_rpc_error(
        grpc.StatusCode.NOT_FOUND,
        inventory_stub.BatchGetProducts,
        inventory_pb2.BatchGetProductsRequest(skus=["MOUSE", "TOASTER"]),
    )
    assert "TOASTER" in error.details()


def test_unknown_sku_error_is_truncated(inventory_stub):
    skus = [f"SKU{index}" for index in range(50)]
    error = assert_rpc_error(
        grpc.StatusCode.NOT_FOUND, inventory_stub.BatchGetProducts, inventory_pb2.BatchGetProductsRequest(skus=skus)
    )
    assert "(+40 more)" in error.details()


def test_oversized_requests_are_rejected(inventory_stub):
    assert_rpc_error(
        grpc.StatusCode.INVALID_ARGUMENT,
        inventory_stub.BatchGetProducts,
        inventory_pb2.BatchGetProductsRequest(skus=[f"SKU{index}" for index in range(101)]),
    )
    assert_rpc_error(grpc.StatusCode.INVALID_ARGUMENT, reserve, inventory_stub, {"MOUSE": 10_001})


def test_reserve_then_release_restores_stock(inventory_stub):
    reserve(inventory_stub, {"KEYBOARD": 2, "MOUSE": 3}, order_id="order-1")
    assert stock_levels(inventory_stub)["KEYBOARD"] == 8

    release(inventory_stub, "order-1")
    release(inventory_stub, "order-1")

    assert stock_levels(inventory_stub)["KEYBOARD"] == 10
    assert stock_levels(inventory_stub)["MOUSE"] == 25


def test_committed_reservation_can_still_be_released_for_cancellation(inventory_stub):
    reserve(inventory_stub, {"MONITOR": 2}, order_id="order-1")
    commit(inventory_stub, "order-1")
    commit(inventory_stub, "order-1")  # idempotent

    release(inventory_stub, "order-1")

    assert stock_levels(inventory_stub)["MONITOR"] == 3


def test_reserve_is_idempotent_per_order_id(inventory_stub):
    first = reserve(inventory_stub, {"MOUSE": 5}, order_id="order-1")
    retried = reserve(inventory_stub, {"MOUSE": 5}, order_id="order-1")

    assert first.reservation_id == retried.reservation_id
    assert stock_levels(inventory_stub)["MOUSE"] == 20


def test_release_before_reserve_fences_late_reservation(inventory_stub):
    release(inventory_stub, "order-1")  # e.g. compensation after an ambiguous ReserveStock timeout

    assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, reserve, inventory_stub, {"MOUSE": 1}, "order-1")
    assert stock_levels(inventory_stub)["MOUSE"] == 25


def test_reserve_after_release_is_rejected(inventory_stub):
    reserve(inventory_stub, {"MOUSE": 1}, order_id="order-1")
    release(inventory_stub, "order-1")
    assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, reserve, inventory_stub, {"MOUSE": 1}, "order-1")


def test_commit_after_release_is_rejected(inventory_stub):
    reserve(inventory_stub, {"MOUSE": 1}, order_id="order-1")
    release(inventory_stub, "order-1")
    assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, commit, inventory_stub, "order-1")


def test_commit_of_unknown_order_is_rejected(inventory_stub):
    assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, commit, inventory_stub, "never-reserved")


def test_insufficient_stock_reserves_nothing(inventory_stub):
    assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, reserve, inventory_stub, {"KEYBOARD": 1, "MONITOR": 4})
    assert stock_levels(inventory_stub)["KEYBOARD"] == 10


def test_concurrent_reservations_never_oversell(inventory_stub):
    def try_reserve_one_monitor(_) -> bool:
        try:
            reserve(inventory_stub, {"MONITOR": 1})
            return True
        except grpc.RpcError as rpc_error:
            assert rpc_error.code() == grpc.StatusCode.FAILED_PRECONDITION
            return False

    with ThreadPoolExecutor(max_workers=10) as executor:
        outcomes = list(executor.map(try_reserve_one_monitor, range(20)))

    assert sum(outcomes) == 3
    assert stock_levels(inventory_stub)["MONITOR"] == 0


def test_concurrent_release_and_reserve_for_same_order_never_leak_stock(inventory_stub):
    def reserve_or_fail(order_id):
        try:
            reserve(inventory_stub, {"MOUSE": 1}, order_id)
        except grpc.RpcError:
            pass

    order_ids = [f"race-{index}" for index in range(20)]
    with ThreadPoolExecutor(max_workers=20) as executor:
        for order_id in order_ids:
            executor.submit(reserve_or_fail, order_id)
            executor.submit(release, inventory_stub, order_id)

    # Whatever the interleaving, every order ends released, so all stock is back.
    for order_id in order_ids:
        release(inventory_stub, order_id)
    assert stock_levels(inventory_stub)["MOUSE"] == 25


@pytest.mark.parametrize("sku", ["MO\x00USE", "A" * 65, ""])
def test_malformed_sku_is_invalid_not_internal(inventory_stub, sku):
    error = assert_rpc_error(
        grpc.StatusCode.INVALID_ARGUMENT, inventory_stub.BatchGetProducts, inventory_pb2.BatchGetProductsRequest(skus=[sku])
    )
    assert len(error.details()) < 100  # the SKU is not echoed back
    assert_rpc_error(grpc.StatusCode.INVALID_ARGUMENT, reserve, inventory_stub, {sku: 1})


@pytest.mark.parametrize("quantity", [0, -1])
def test_non_positive_quantity_is_invalid(inventory_stub, quantity):
    assert_rpc_error(grpc.StatusCode.INVALID_ARGUMENT, reserve, inventory_stub, {"MOUSE": quantity})


class TestReservationExpiry:
    @pytest.fixture
    def reservation_ttl(self) -> timedelta:
        return timedelta(seconds=1)

    def test_expired_hold_is_swept_and_cannot_be_committed(self, inventory_service, inventory_stub):
        reserve(inventory_stub, {"MONITOR": 3}, order_id="order-1")
        reserve(inventory_stub, {"MOUSE": 1}, order_id="order-2")
        commit(inventory_stub, "order-2")
        time.sleep(1.2)

        assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, commit, inventory_stub, "order-1")
        assert inventory_service.repository.release_expired() == 1

        levels = stock_levels(inventory_stub)
        assert levels["MONITOR"] == 3  # expired hold returned
        assert levels["MOUSE"] == 24  # committed reservation never expires


def admin_metadata(roles=("admin",)) -> list[tuple[str, str]]:
    claims = json.dumps({"sub": "ops", "roles": list(roles)}).encode()
    return [("x-jwt-payload", base64.urlsafe_b64encode(claims).rstrip(b"=").decode())]


class TestRestock:
    @pytest.fixture
    def envoy_stub(self, inventory_service):
        with inventory_service.channel("envoy") as channel:
            yield inventory_pb2_grpc.InventoryServiceStub(channel)

    def restock(self, stub, sku="MOUSE", quantity=5, metadata=None):
        return stub.RestockProduct(
            inventory_pb2.RestockProductRequest(sku=sku, quantity=quantity),
            metadata=admin_metadata() if metadata is None else metadata,
        )

    def test_admin_via_edge_can_restock(self, envoy_stub, inventory_stub):
        product = self.restock(envoy_stub, quantity=5)
        assert product.available_quantity == 30
        assert stock_levels(inventory_stub)["MOUSE"] == 30

    @pytest.mark.parametrize("metadata", [[], admin_metadata(roles=("viewer",))])
    def test_non_admin_is_denied(self, envoy_stub, metadata):
        assert_rpc_error(grpc.StatusCode.PERMISSION_DENIED, self.restock, envoy_stub, "MOUSE", 5, metadata)

    def test_internal_caller_cannot_restock_even_with_admin_claims(self, inventory_stub):
        # Order is allowed to call inventory, but must not be able to forge an admin request.
        assert_rpc_error(grpc.StatusCode.PERMISSION_DENIED, self.restock, inventory_stub)

    @pytest.mark.parametrize("quantity", [0, -1, 100_001])
    def test_quantity_bounds(self, envoy_stub, quantity):
        assert_rpc_error(grpc.StatusCode.INVALID_ARGUMENT, self.restock, envoy_stub, "MOUSE", quantity)

    def test_malformed_sku_is_invalid(self, envoy_stub):
        assert_rpc_error(grpc.StatusCode.INVALID_ARGUMENT, self.restock, envoy_stub, "MO\x00USE", 5)

    def test_total_stock_is_capped(self, envoy_stub):
        for _ in range(9):
            self.restock(envoy_stub, quantity=100_000)
        assert_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, self.restock, envoy_stub, "MOUSE", 100_000)

    def test_unknown_sku(self, envoy_stub):
        assert_rpc_error(grpc.StatusCode.NOT_FOUND, self.restock, envoy_stub, "TOASTER")
