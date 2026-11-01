"""mTLS and per-caller authorization."""

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from shop.inventory.v1 import inventory_pb2, inventory_pb2_grpc
from shop.payment.v1 import payment_pb2, payment_pb2_grpc

CHARGE_REQUEST = payment_pb2.ChargeRequest(idempotency_key="k", customer_id="c", amount_cents=100)
CANCEL_REQUEST = payment_pb2.CancelChargeRequest(idempotency_key="k")


@pytest.mark.parametrize("caller", ["envoy", "dev-client", "inventory"])
def test_payment_rejects_callers_other_than_order(payment_service, caller):
    with payment_service.channel(caller) as channel:
        with pytest.raises(grpc.RpcError) as error_info:
            payment_pb2_grpc.PaymentServiceStub(channel).Charge(CHARGE_REQUEST)
    assert error_info.value.code() == grpc.StatusCode.PERMISSION_DENIED


def test_envoy_may_list_inventory_products(inventory_service):
    with inventory_service.channel("envoy") as channel:
        products = list(inventory_pb2_grpc.InventoryServiceStub(channel).ListProducts(inventory_pb2.ListProductsRequest()))
    assert products


def test_connection_without_client_certificate_fails(payment_service):
    credentials = grpc.ssl_channel_credentials(root_certificates=(payment_service.certs_dir / "ca.crt").read_bytes())
    with grpc.secure_channel(f"localhost:{payment_service.handle.port}", credentials) as channel:
        with pytest.raises(grpc.RpcError) as error_info:
            payment_pb2_grpc.PaymentServiceStub(channel).Charge(CHARGE_REQUEST, timeout=3)
    assert error_info.value.code() == grpc.StatusCode.UNAVAILABLE


def test_plaintext_connection_fails(payment_service):
    with grpc.insecure_channel(f"localhost:{payment_service.handle.port}") as channel:
        with pytest.raises(grpc.RpcError) as error_info:
            payment_pb2_grpc.PaymentServiceStub(channel).Charge(CHARGE_REQUEST, timeout=3)
    assert error_info.value.code() == grpc.StatusCode.UNAVAILABLE


def test_health_check_is_open_to_any_trusted_peer(payment_service):
    with payment_service.channel("dev-client") as channel:
        response = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest())
    assert response.status == health_pb2.HealthCheckResponse.SERVING


@pytest.mark.parametrize("caller", ["envoy", "dev-client"])
def test_only_order_may_cancel_charges(payment_service, caller):
    with payment_service.channel(caller) as channel:
        with pytest.raises(grpc.RpcError) as error_info:
            payment_pb2_grpc.PaymentServiceStub(channel).CancelCharge(CANCEL_REQUEST)
    assert error_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
