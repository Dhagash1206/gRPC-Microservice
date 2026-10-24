"""Black-box tests against the running docker compose stack, through Envoy only.

Run with `make up && make e2e`. Assertions are relative, so the suite can run repeatedly.
"""

import base64
import hashlib
import hmac
import importlib.util
import json
import os
import pathlib
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import grpc
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from shop.order.v1 import order_pb2, order_pb2_grpc

pytestmark = pytest.mark.e2e

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
CA_FILE = PROJECT_ROOT / "certs/ca.crt"
EDGE_HOST = os.environ.get("E2E_EDGE_HOST", "localhost:8443")
BASE_URL = f"https://{EDGE_HOST}"
MAILPIT_URL = os.environ.get("E2E_MAILPIT_URL", "http://127.0.0.1:8025")

_dev_auth_spec = importlib.util.spec_from_file_location("dev_auth", PROJECT_ROOT / "scripts/dev_auth.py")
dev_auth = importlib.util.module_from_spec(_dev_auth_spec)
_dev_auth_spec.loader.exec_module(dev_auth)


def issue_token(customer_id: str, ttl_minutes: int = 5, roles: list[str] | None = None) -> str:
    return dev_auth.issue_token(PROJECT_ROOT / "certs/jwt_private.pem", customer_id, ttl_minutes, roles)


def unique_customer(prefix: str) -> str:
    return f"{prefix}-{time.time_ns()}"


def http_request(
    method: str, path: str, token: str | None = None, body: dict | bytes | None = None, headers: dict | None = None
) -> tuple[int, object, dict]:
    all_headers = {"Content-Type": "application/json", **(headers or {})}
    if token:
        all_headers["Authorization"] = f"Bearer {token}"
    data = body if isinstance(body, bytes) or body is None else json.dumps(body).encode()
    request = urllib.request.Request(BASE_URL + path, data=data, headers=all_headers, method=method)
    ssl_context = ssl.create_default_context(cafile=str(CA_FILE))
    try:
        with urllib.request.urlopen(request, context=ssl_context, timeout=15) as response:
            status_code, raw_body, response_headers = response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as http_error:
        status_code, raw_body, response_headers = http_error.code, http_error.read(), dict(http_error.headers)
    try:
        parsed_body = json.loads(raw_body)
    except json.JSONDecodeError:
        parsed_body = raw_body.decode(errors="replace")
    return status_code, parsed_body, {key.lower(): value for key, value in response_headers.items()}


def stock_levels() -> dict[str, int]:
    status_code, products, _ = http_request("GET", "/v1/products")
    assert status_code == 200, products
    return {product["sku"]: product["availableQuantity"] for product in products}


def create_order(token: str, items: dict[str, int]) -> tuple[int, object]:
    body = {"items": [{"sku": sku, "quantity": quantity} for sku, quantity in items.items()]}
    status_code, response_body, _ = http_request("POST", "/v1/orders", token=token, body=body)
    return status_code, response_body


def wait_for_email(recipient_prefix: str, subject_fragment: str, timeout_seconds: float = 20) -> dict:
    query = urllib.parse.quote(f'to:"{recipient_prefix}" subject:"{subject_fragment}"')
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with urllib.request.urlopen(f"{MAILPIT_URL}/api/v1/search?query={query}", timeout=5) as response:
            messages = json.loads(response.read())["messages"]
        if messages:
            return messages[0]
        time.sleep(0.5)
    pytest.fail(f"no email to {recipient_prefix} with subject containing {subject_fragment!r}")


def compose_exec(service: str, *command: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "exec", "-T", service, *command],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60,
    )


@pytest.fixture(scope="session", autouse=True)
def replenish_stock():
    """Tops stock up through the admin API so the suite can run any number of times."""
    admin_token = issue_token("e2e-admin", roles=["admin"])
    for sku, target in {"MOUSE": 500, "KEYBOARD": 200}.items():
        shortfall = target - stock_levels()[sku]
        if shortfall > 0:
            status_code, body, _ = http_request(
                "POST", f"/v1/products/{sku}:restock", token=admin_token, body={"quantity": shortfall}
            )
            assert status_code == 200, body


# ---- Catalog and authentication ----


def test_products_are_public():
    assert {"KEYBOARD", "MOUSE", "MONITOR", "LAPTOP"} <= stock_levels().keys()


def test_orders_require_a_token():
    status_code, _ = create_order("", {"MOUSE": 1})
    assert status_code == 401


def test_token_signed_by_unknown_key_is_rejected():
    attacker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged_token = jwt.encode(
        {"iss": dev_auth.ISSUER, "aud": dev_auth.AUDIENCE, "sub": "customer-42", "exp": int(time.time()) + 300},
        attacker_key,
        algorithm="RS256",
        headers={"kid": dev_auth.KEY_ID},
    )
    status_code, _ = create_order(forged_token, {"MOUSE": 1})
    assert status_code == 401


def test_expired_token_is_rejected():
    # Envoy tolerates 60s of clock skew by default, so expire well beyond that.
    status_code, _ = create_order(issue_token("customer-42", ttl_minutes=-5), {"MOUSE": 1})
    assert status_code == 401


def test_restock_requires_admin_role():
    path = "/v1/products/MOUSE:restock"
    assert http_request("POST", path, body={"quantity": 1})[0] == 401
    status_code, body, _ = http_request("POST", path, token=issue_token("shopper"), body={"quantity": 1})
    assert status_code == 403 and "RBAC" in str(body)
    status_code, product, _ = http_request(
        "POST", path, token=issue_token("ops", roles=["admin"]), body={"quantity": 1}
    )
    assert status_code == 200 and product["sku"] == "MOUSE"


def test_algorithm_confusion_token_is_rejected():
    # Classic attack: sign HS256 using the server's *public* RSA key as the HMAC secret.
    public_key_pem = (PROJECT_ROOT / "certs/jwt_public.pem").read_bytes()
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "kid": dev_auth.KEY_ID}).encode()).rstrip(b"=")
    claims = {"iss": dev_auth.ISSUER, "aud": dev_auth.AUDIENCE, "sub": "customer-42", "exp": int(time.time()) + 300}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    signature = base64.urlsafe_b64encode(
        hmac.new(public_key_pem, header + b"." + payload, hashlib.sha256).digest()
    ).rstrip(b"=")
    status_code, _ = create_order((header + b"." + payload + b"." + signature).decode(), {"MOUSE": 1})
    assert status_code == 401


def test_token_for_another_audience_is_rejected():
    private_key = (PROJECT_ROOT / "certs/jwt_private.pem").read_bytes()
    claims = {"iss": dev_auth.ISSUER, "aud": "some-other-api", "sub": "customer-42", "exp": int(time.time()) + 300}
    token = jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": dev_auth.KEY_ID})
    status_code, body = create_order(token, {"MOUSE": 1})
    # Envoy answers a validly signed token for the wrong audience with 403, not 401.
    assert status_code == 403 and "Audiences" in body


def test_forged_identity_header_is_ignored():
    attacker = unique_customer("attacker")
    forged_payload = base64.urlsafe_b64encode(b'{"sub":"victim"}').rstrip(b"=").decode()
    status_code, order, _ = http_request(
        "POST", "/v1/orders", token=issue_token(attacker),
        body={"items": [{"sku": "MOUSE", "quantity": 1}]}, headers={"x-jwt-payload": forged_payload},
    )
    assert status_code == 200, order
    assert order["customerId"] == attacker


def test_unsafe_subject_is_rejected():
    status_code, _ = create_order(issue_token("evil\r\nBcc: all@example.com"), {"MOUSE": 1})
    assert status_code == 401


# ---- Order lifecycle ----


def test_create_get_and_list_orders_over_rest():
    customer_id = unique_customer("lister")
    token = issue_token(customer_id)
    stock_before = stock_levels()

    created_ids = []
    for _ in range(3):
        status_code, order = create_order(token, {"MOUSE": 1})
        assert status_code == 200, order
        assert order["status"] == "ORDER_STATUS_CONFIRMED"
        created_ids.append(order["id"])
    assert stock_levels()["MOUSE"] == stock_before["MOUSE"] - 3

    status_code, fetched, _ = http_request("GET", f"/v1/orders/{created_ids[0]}", token=token)
    assert status_code == 200 and fetched["id"] == created_ids[0]

    status_code, first_page, _ = http_request("GET", "/v1/orders?pageSize=2", token=token)
    assert status_code == 200, first_page
    status_code, second_page, _ = http_request(
        "GET", f"/v1/orders?pageSize=2&pageToken={first_page['nextPageToken']}", token=token
    )
    listed_ids = [order["id"] for order in first_page["orders"] + second_page["orders"]]
    assert listed_ids == list(reversed(created_ids))
    assert second_page["nextPageToken"] == ""


def test_customers_cannot_read_or_cancel_each_others_orders():
    _, order = create_order(issue_token(unique_customer("owner")), {"MOUSE": 1})
    intruder_token = issue_token(unique_customer("intruder"))

    status_code, _, _ = http_request("GET", f"/v1/orders/{order['id']}", token=intruder_token)
    assert status_code == 404
    status_code, _, _ = http_request("POST", f"/v1/orders/{order['id']}:cancel", token=intruder_token, body={})
    assert status_code == 404

    status_code, page, _ = http_request("GET", "/v1/orders", token=intruder_token)
    assert page["orders"] == []


def test_cancel_order_refunds_restocks_and_emails():
    customer_id = unique_customer("canceller")
    token = issue_token(customer_id)
    _, order = create_order(token, {"KEYBOARD": 2})
    stock_after_order = stock_levels()["KEYBOARD"]

    status_code, cancelled, _ = http_request("POST", f"/v1/orders/{order['id']}:cancel", token=token, body={})
    assert status_code == 200, cancelled
    assert cancelled["status"] == "ORDER_STATUS_CANCELLED"
    assert stock_levels()["KEYBOARD"] == stock_after_order + 2

    status_code, again, _ = http_request("POST", f"/v1/orders/{order['id']}:cancel", token=token, body={})
    assert status_code == 200 and again["status"] == "ORDER_STATUS_CANCELLED"
    wait_for_email(customer_id, "cancelled")


def test_retry_with_idempotency_key_does_not_charge_twice():
    token = issue_token(unique_customer("retrier"))
    body = {"items": [{"sku": "MOUSE", "quantity": 1}], "idempotencyKey": f"checkout-{time.time_ns()}"}
    stock_before = stock_levels()["MOUSE"]

    first_status, first, _ = http_request("POST", "/v1/orders", token=token, body=body)
    retry_status, retried, _ = http_request("POST", "/v1/orders", token=token, body=body)

    assert (first_status, retry_status) == (200, 200), (first, retried)
    assert retried["id"] == first["id"] and retried["paymentId"] == first["paymentId"]
    assert stock_levels()["MOUSE"] == stock_before - 1

    mismatched = {**body, "items": [{"sku": "MOUSE", "quantity": 2}]}
    status_code, _, _ = http_request("POST", "/v1/orders", token=token, body=mismatched)
    assert status_code == 400


def test_out_of_stock_fails_without_side_effects():
    stock_before = stock_levels()
    status_code, error_body = create_order(issue_token(unique_customer("buyer")), {"MONITOR": 1_000})
    assert status_code == 400
    assert "insufficient stock" in error_body["message"]
    assert stock_levels() == stock_before


def test_declined_payment_releases_stock_and_records_failed_order():
    customer_id = unique_customer("declined")
    token = issue_token(customer_id)
    stock_before = stock_levels()

    status_code, error_body = create_order(token, {"LAPTOP": 1})

    assert status_code == 400
    assert "declined" in error_body["message"]
    assert stock_levels() == stock_before
    _, page, _ = http_request("GET", "/v1/orders", token=token)
    assert [order["status"] for order in page["orders"]] == ["ORDER_STATUS_FAILED"]
    assert "declined" in page["orders"][0]["failureReason"]


def test_confirmation_email_is_delivered():
    customer_id = unique_customer("mailme")
    _, order = create_order(issue_token(customer_id), {"MOUSE": 1})
    message = wait_for_email(customer_id, order["id"])
    assert message["To"][0]["Address"] == f"{customer_id}@customers.grpcshop.local"


def test_native_grpc_clients_use_the_same_edge():
    credentials = grpc.ssl_channel_credentials(root_certificates=CA_FILE.read_bytes())
    with grpc.secure_channel(EDGE_HOST, credentials) as channel:
        order_stub = order_pb2_grpc.OrderServiceStub(channel)
        request = order_pb2.CreateOrderRequest(items=[order_pb2.OrderItem(sku="MOUSE", quantity=1)])

        with pytest.raises(grpc.RpcError) as error_info:
            order_stub.CreateOrder(request, timeout=10)
        assert error_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

        order = order_stub.CreateOrder(
            request, metadata=[("authorization", f"Bearer {issue_token('grpc-customer')}")], timeout=10
        )
        assert order.customer_id == "grpc-customer"
        assert order.status == order_pb2.ORDER_STATUS_CONFIRMED


# ---- Input validation and edge protection ----


def test_quantity_overflow_is_rejected():
    status_code, error_body = create_order(issue_token("overflow"), {"MOUSE": 2147483647})
    assert status_code == 400, error_body
    status_code, error_body, _ = http_request(
        "POST", "/v1/orders", token=issue_token("overflow"),
        body={"items": [{"sku": "MOUSE", "quantity": 2147483647}] * 2 + [{"sku": "MOUSE", "quantity": 3}]},
    )
    assert status_code == 400, error_body


def test_oversized_body_is_rejected_at_the_edge():
    huge_body = json.dumps({"items": [{"sku": "A" * 100_000, "quantity": 1}]}).encode()
    status_code, _, _ = http_request("POST", "/v1/orders", token=issue_token("big"), body=huge_body)
    assert status_code == 413


def test_unknown_query_parameters_are_rejected():
    status_code, _, _ = http_request("GET", "/v1/orders?admin=true", token=issue_token("query"))
    assert status_code == 400


def test_internal_rpcs_are_not_routable():
    for rpc_path in ("/shop.inventory.v1.InventoryService/ReserveStock", "/shop.payment.v1.PaymentService/Charge"):
        status_code, _, headers = http_request(
            "POST", rpc_path, body=b"", headers={"content-type": "application/grpc"}
        )
        assert headers.get("grpc-status") == "12" or status_code == 404, (rpc_path, status_code, headers)


def test_security_headers_are_set():
    _, _, headers = http_request("GET", "/v1/products")
    assert headers["server"] == "grpcshop"
    assert headers["x-content-type-options"] == "nosniff"
    assert "max-age" in headers["strict-transport-security"]


def test_cors_preflight_allows_only_configured_origin():
    allowed_status, _, allowed_headers = http_request(
        "OPTIONS", "/v1/orders",
        headers={"origin": "http://localhost:3000", "access-control-request-method": "POST"},
    )
    assert allowed_headers.get("access-control-allow-origin") == "http://localhost:3000", (allowed_status, allowed_headers)
    _, _, denied_headers = http_request(
        "OPTIONS", "/v1/orders",
        headers={"origin": "https://evil.example", "access-control-request-method": "POST"},
    )
    assert "access-control-allow-origin" not in denied_headers


# ---- Infrastructure isolation ----


@pytest.mark.parametrize("port", [50051, 50052, 50053, 5432, 5671, 5672])
def test_internal_ports_are_not_published(port):
    with pytest.raises(OSError):
        socket.create_connection(("localhost", port), timeout=1).close()


@pytest.mark.parametrize(
    "service, unreachable_host",
    [("inventory", "payment-db"), ("payment", "inventory-db"), ("inventory", "rabbitmq"), ("payment", "order")],
)
def test_networks_are_segmented(service, unreachable_host):
    probe = (
        "import socket,sys\n"
        "try:\n"
        f"    socket.create_connection(('{unreachable_host}', 5432 if '-db' in '{unreachable_host}' else 5671), timeout=2)\n"
        "    print('REACHABLE')\n"
        "except OSError as error:\n"
        "    print('blocked:', error)\n"
    )
    result = compose_exec(service, "python", "-c", probe)
    assert "blocked" in result.stdout, result.stdout + result.stderr


def test_databases_refuse_plaintext_and_passwordless_tcp_logins():
    plaintext = compose_exec("order-db", "psql", "host=localhost user=order dbname=order sslmode=disable", "-c", "select 1")
    assert plaintext.returncode != 0 and "no encryption" in plaintext.stderr
    without_certificate = compose_exec(
        "order-db", "psql", "host=localhost user=postgres dbname=order sslmode=require", "-c", "select 1"
    )
    assert without_certificate.returncode != 0 and "certificate" in without_certificate.stderr


# Last: drains the shared rate-limit bucket.
def test_burst_traffic_is_rate_limited():
    def fetch_products(_) -> int:
        return http_request("GET", "/v1/products")[0]

    with ThreadPoolExecutor(max_workers=50) as executor:
        status_codes = list(executor.map(fetch_products, range(600)))

    assert 429 in status_codes
    assert 200 in status_codes
    time.sleep(3)  # let the bucket refill for whatever runs next
