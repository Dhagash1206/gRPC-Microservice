#!/usr/bin/env bash
# Walks through the public REST API exposed by Envoy. Requires `make up`.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
BASE_URL="${BASE_URL:-https://localhost:8443}"

call() {
  local title="$1"; shift
  printf '\n\033[1m== %s ==\033[0m\n' "$title"
  curl -sS --cacert "$PROJECT_ROOT/certs/ca.crt" -w '\n[HTTP %{http_code}]\n' "$@"
}

token="$("$PYTHON" "$PROJECT_ROOT/scripts/dev_auth.py" token --sub customer-42)"
other_token="$("$PYTHON" "$PROJECT_ROOT/scripts/dev_auth.py" token --sub customer-7)"

call "List products (public, server-streaming RPC as a JSON array)" "$BASE_URL/v1/products"

call "Create order without a token -> 401" -X POST "$BASE_URL/v1/orders" \
  -d '{"items":[{"sku":"MOUSE","quantity":1}]}'

order_json="$(curl -sS --cacert "$PROJECT_ROOT/certs/ca.crt" -X POST "$BASE_URL/v1/orders" \
  -H "Authorization: Bearer $token" -d '{"items":[{"sku":"MOUSE","quantity":2},{"sku":"KEYBOARD","quantity":1}]}')"
printf '\n\033[1m== Create order as customer-42 ==\033[0m\n%s\n' "$order_json"
order_id="$("$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$order_json")"

call "Get own order" -H "Authorization: Bearer $token" "$BASE_URL/v1/orders/$order_id"

call "Get the same order as customer-7 -> 404" -H "Authorization: Bearer $other_token" "$BASE_URL/v1/orders/$order_id"

call "Out of stock -> 400 (FAILED_PRECONDITION)" -X POST "$BASE_URL/v1/orders" \
  -H "Authorization: Bearer $token" -d '{"items":[{"sku":"MONITOR","quantity":1000}]}'

call "Payment declined -> 400, reserved stock released" -X POST "$BASE_URL/v1/orders" \
  -H "Authorization: Bearer $token" -d '{"items":[{"sku":"LAPTOP","quantity":1}]}'

printf '\n\033[1m== Async: notification service received OrderConfirmed ==\033[0m\n'
sleep 2
docker compose -f "$PROJECT_ROOT/docker-compose.yml" logs notification --no-log-prefix | grep "$order_id" || echo "(not yet received)"
