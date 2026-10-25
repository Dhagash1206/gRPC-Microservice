#!/usr/bin/env bash
# Creates a dev CA, one mTLS cert per workload, and the JWT signing keypair.
# Usage: gen_certs.sh [output_dir]   (default: certs/)
# DEV ONLY: keys are world-readable so containers running as non-root can mount them.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$PROJECT_ROOT/certs}"
PYTHON="${PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
# Service identities (CN = gRPC caller name and database role) plus infrastructure servers.
WORKLOADS=(envoy order inventory payment notification dev-client
  inventory-db payment-db order-db notification-db rabbitmq mailpit otel-collector)
DAYS=825

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key 2>/dev/null
# Strict X.509 validators (e.g. Python 3.13+ default SSL context) require these extensions.
openssl req -x509 -new -key ca.key -sha256 -days "$DAYS" -subj "/CN=grpcshop-dev-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -out ca.crt

for workload in "${WORKLOADS[@]}"; do
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$workload.key" 2>/dev/null
  openssl req -new -key "$workload.key" -subj "/CN=$workload" -out "$workload.csr"
  # The CN is the workload identity used for authorization; SANs let the same
  # cert work for compose DNS names and for localhost during local runs/tests.
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "subjectAltName=DNS:$workload,DNS:localhost,IP:127.0.0.1"
    echo "extendedKeyUsage=serverAuth,clientAuth"
    echo "keyUsage=critical,digitalSignature"
    echo "subjectKeyIdentifier=hash"
    echo "authorityKeyIdentifier=keyid,issuer"
  } > "$workload.ext"
  openssl x509 -req -in "$workload.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -extfile "$workload.ext" -out "$workload.crt" 2>/dev/null
  rm "$workload.csr" "$workload.ext"
done
rm -f ca.srl

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out jwt_private.pem 2>/dev/null
openssl pkey -in jwt_private.pem -pubout -out jwt_public.pem
"$PYTHON" "$PROJECT_ROOT/scripts/dev_auth.py" jwks --public-key jwt_public.pem > jwks.json

chmod 644 ./*
echo "certs + JWT keys -> $OUT_DIR"
