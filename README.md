# grpcshop: gRPC microservices (Go + Python)

A checkout system built from microservices. Services call each other over **gRPC with mutual TLS**, each one owns its **own Postgres database**, an **Envoy** edge exposes a JWT-protected REST and gRPC API, and order events flow asynchronously through **RabbitMQ**.

```
                       HTTPS :8443 (REST/JSON or gRPC) + JWT
  client ─────────────────────────────► Envoy  (TLS termination, JWT validation, REST⇄gRPC transcoding)
                                          │ mTLS
                 ┌────────────────────────┴───────────────┐
                 ▼                                        ▼ (GET /v1/products only)
          Order service (Go) ──────── mTLS ───────► Inventory service (Python) ── inventory-db
          │   │                                      
          │   └────────────────────── mTLS ───────► Payment service (Python) ──── payment-db
          │
          ├── order-db  (orders + transactional outbox)
          │
          └── outbox relay ──► RabbitMQ  shop.events ──► Notification service (Go)
                                          └─ dead-letter queue
```

## Quick start

Requires Docker, Go 1.26+, Python 3.12+.

```bash
make setup     # venv, Python deps, Go protoc plugins, dev certs, generated code
make up        # build + start all 9 containers, wait until healthy
make demo      # scripted REST walkthrough
make e2e       # end-to-end tests through Envoy
make down      # stop (make clean also deletes the databases)
```

Use the API by hand:

```bash
TOKEN=$(make -s token SUB=customer-42)
curl --cacert certs/ca.crt https://localhost:8443/v1/products
curl --cacert certs/ca.crt -X POST https://localhost:8443/v1/orders \
  -H "Authorization: Bearer $TOKEN" -d '{"items":[{"sku":"MOUSE","quantity":2}]}'
curl --cacert certs/ca.crt -H "Authorization: Bearer $TOKEN" https://localhost:8443/v1/orders/<id>
```

- The RabbitMQ UI is at http://localhost:15672 (`app` / `app`).
- Seeded products: `KEYBOARD`, `MOUSE`, `MONITOR` (3 in stock), and `LAPTOP` ($1499.99). Payment declines charges over $1000, so ordering a `LAPTOP` demonstrates the release-stock path.

## Services

| Service | Lang | gRPC API | Owns | Allowed callers (mTLS CN) |
|---|---|---|---|---|
| **order** | Go | `OrderService.CreateOrder`, `GetOrder` | `order` DB: orders, order_lines, outbox | `envoy` (+ `dev-client` in compose) |
| **inventory** | Python | `BatchGetProducts`, `ListProducts` (stream), `ReserveStock`, `ReleaseStock` | `inventory` DB: products, reservations | `order`, `envoy` |
| **payment** | Python | `Charge` | `payment` DB: payments | `order` |
| **notification** | Go | (none; consumes `order.confirmed` events) | | |
| **envoy** | | Public edge: `POST /v1/orders`, `GET /v1/orders/{id}`, `GET /v1/products` | | |

## How a checkout works (saga)

`CreateOrder` in `go/internal/order/service.go`:

1. Get the customer ID from the JWT `sub` claim, which Envoy has already verified and forwards as `x-jwt-payload`.
2. `Inventory.BatchGetProducts`: get current prices in one call rather than one call per product.
3. `Inventory.ReserveStock(order_id)`: an all-or-nothing reservation, made safe to retry by keying it on `order_id`.
4. `Payment.Charge(idempotency_key = order_id)`. If the charge fails, call `Inventory.ReleaseStock`. This undo step runs even if the client disconnects.
5. Insert the order, its lines and an `OrderConfirmed` outbox row in **one transaction**.
6. A background relay publishes outbox rows to RabbitMQ. If RabbitMQ is down, orders still succeed and the events go out once it recovers (verified).

## Production patterns used

| Concern | Implementation | Where |
|---|---|---|
| Encryption in transit | Envoy terminates TLS 1.2+ at the edge; services use TLS 1.3 mTLS with each other, client certs required | `scripts/gen_certs.sh`, `go/internal/tlsconfig`, `python/src/common/tls.py` |
| Service-to-service authz | Each service allowlists caller identities (cert CN); health and reflection are open to any CA-signed peer | `grpcmw.UnaryServerPeerAuthorization`, `PeerAuthorizationInterceptor` |
| Server identity pinning | Envoy checks each upstream's certificate name (SAN), so a valid cert alone is not enough | `envoy/envoy.yaml` |
| End-user auth | RS256 JWT checked at the edge (issuer, audience, expiry, signature); only the verified claims are forwarded | `envoy/envoy.yaml`, `go/internal/identity` |
| Resource ownership | Customers see only their own orders; others' orders return NOT_FOUND so IDs can't be guessed | `Service.GetOrder` |
| REST and gRPC on one API | Envoy JSON transcoding driven by `google.api.http` annotations | `proto/shop/*/v1/*.proto` |
| Database per service | 3 Postgres instances with versioned migrations; an advisory lock keeps replicas from migrating at once | `*/migrations/`, `go/internal/migrate`, `python/src/common/db.py` |
| No overselling | Rows locked in SKU order (no deadlocks); a `CHECK (available_quantity >= 0)` constraint as backstop; tested with 20 concurrent buyers | `inventory_service/repository.py` |
| Safe retries | Client retries on UNAVAILABLE; every write is keyed so repeats have no extra effect, and payment declines are stored and replayed | `cmd/order/main.go`, `payment_service/repository.py` |
| Timeouts | Each downstream call has its own timeout, but never outlasts the caller's deadline | `callWithTimeout` |
| Error mapping | Business errors pass through; infrastructure failures become `UNAVAILABLE` without leaking internal details | `Service.downstreamError` |
| Reliable events | Transactional outbox; `FOR UPDATE SKIP LOCKED` (safe with several order replicas); publisher confirms + mandatory flag; exponential backoff | `go/internal/outbox`, `go/internal/rabbitmq` |
| Consumer resilience | Durable quorum queue; manual acks; poison messages go to the dead-letter queue; failed deliveries retried up to 5 times; duplicates detected by event ID; reconnects automatically | `go/internal/notification` |
| Tracing | Envoy's `x-request-id` is logged by every service and carried through the event headers, so one ID links all the log lines for a request | `grpcmw`, `common/interceptors.py` |
| Health / lifecycle | gRPC health checks (Docker and Envoy active checks); on shutdown, NOT_SERVING first, then in-flight calls finish | `common/server.py`, `cmd/order/main.go` |
| Scaling | Envoy uses `STRICT_DNS` with round-robin, so `docker compose up --scale order=3` works | `envoy/envoy.yaml` |
| Minimal images | Go: distroless/nonroot; Python: slim, non-root, with code generated at build time | `go/Dockerfile`, `python/Dockerfile` |

## Layout

```
proto/shop/{order,inventory,payment,events}/v1/   API + event contracts (source of truth)
proto/third_party/google/api/                     vendored HTTP annotations
go/                       module grpcshop
  cmd/order, cmd/notification
  internal/{order,outbox,rabbitmq,notification,grpcmw,identity,tlsconfig,migrate}
  gen/                    generated Go stubs (committed)
python/
  src/common/             mTLS, interceptors, server bootstrap, DB pool + migrations
  src/inventory_service/  src/payment_service/   (each: servicer, repository, migrations, main)
  tests/                  integration tests (real Postgres via testcontainers)
  tests/e2e/              black-box tests against the compose stack
envoy/                    edge config + Dockerfile (builds the proto descriptor)
scripts/                  gen_certs.sh, dev_auth.py (dev JWT issuer), generate_protos.sh, demo.sh
docker-compose.yml
```

## Tests

| Suite | Command | Covers |
|---|---|---|
| Python integration (22) | `make test-python` | Real Postgres. Reservations, idempotency, concurrent overselling, payment replay, mTLS rejection, per-caller authorization |
| Go unit | `make test-go` | Saga steps, release-stock path, error mapping, input validation, order ownership, JWT identity, event deduplication and dead-lettering |
| End-to-end (15) | `make e2e` | Through Envoy: REST + native gRPC, JWT missing/forged/expired, cross-customer access, stock effects, internal ports not exposed, async notification |

## Changing the API

Edit `proto/`, run `make proto` (regenerates Python, Go and the Envoy descriptor), then `make up`. Keep changes backward compatible: add fields, never renumber or reuse them. Breaking changes go in a new `v2` package.

## Not production-ready yet

- **Dev secrets:** certs come from a local dev CA, private keys are world-readable, and database/broker passwords are in compose. Use a secret manager plus cert rotation (e.g. cert-manager or a SPIFFE/SPIRE service mesh).
- **Unencrypted Postgres and RabbitMQ:** connections stay on the internal Docker network without TLS. Turn on `sslmode=verify-full` and `amqps`.
- **Fake identity provider:** `dev_auth.py` stands in for a real IdP (Auth0, Keycloak, Cognito); point Envoy's `remote_jwks` at it.
- **Payment timeouts:** a timed-out charge has an unknown outcome and currently releases stock right away. Production should check with the payment provider via the idempotency key before doing that.
- **Abandoned reservations:** reservations need a timeout and cleanup job in case the order service crashes partway through a checkout.
- **In-memory dedupe:** Notification's duplicate check is in memory, so multiple replicas need a shared store.
- **Observability:** add OpenTelemetry traces and metrics, and alerts on outbox lag and dead-letter queue depth.
