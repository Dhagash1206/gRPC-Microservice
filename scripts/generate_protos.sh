#!/usr/bin/env bash
# Generates code from proto/:
#   python  -> python/src/shop        (gitignored, also generated inside the Docker build)
#   go      -> go/gen                 (committed, as is conventional for Go)
#   envoy   -> envoy/descriptors.pb   (gitignored, also generated inside the Docker build)
# Usage: generate_protos.sh [python|go|envoy|all]
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
TARGET="${1:-all}"
PROTO_DIR="$PROJECT_ROOT/proto"
SHOP_PROTOS=("$PROTO_DIR"/shop/*/v1/*.proto)
PROTOC=("$PYTHON" -m grpc_tools.protoc --proto_path="$PROTO_DIR" --proto_path="$PROTO_DIR/third_party")

generate_python() {
  local output_dir="$PROJECT_ROOT/python/src"
  mkdir -p "$output_dir"
  # google/api Python modules come from the googleapis-common-protos package, so only shop/* is generated.
  "${PROTOC[@]}" --python_out="$output_dir" --pyi_out="$output_dir" --grpc_python_out="$output_dir" "${SHOP_PROTOS[@]}"
  find "$output_dir/shop" -type d -exec touch {}/__init__.py \;
  echo "python stubs -> python/src/shop"
}

generate_go() {
  local go_bin
  go_bin="$(go env GOPATH)/bin"
  rm -rf "$PROJECT_ROOT/go/gen"
  mkdir -p "$PROJECT_ROOT/go/gen"
  "${PROTOC[@]}" \
    --plugin=protoc-gen-go="$go_bin/protoc-gen-go" \
    --plugin=protoc-gen-go-grpc="$go_bin/protoc-gen-go-grpc" \
    --go_out="$PROJECT_ROOT/go" --go_opt=module=grpcshop \
    --go-grpc_out="$PROJECT_ROOT/go" --go-grpc_opt=module=grpcshop \
    "${SHOP_PROTOS[@]}"
  echo "go stubs -> go/gen"
}

generate_envoy() {
  "${PROTOC[@]}" --include_imports --descriptor_set_out="$PROJECT_ROOT/envoy/descriptors.pb" \
    "$PROTO_DIR/shop/order/v1/order.proto" "$PROTO_DIR/shop/inventory/v1/inventory.proto"
  echo "envoy descriptor -> envoy/descriptors.pb"
}

case "$TARGET" in
  python) generate_python ;;
  go) generate_go ;;
  envoy) generate_envoy ;;
  all) generate_python; generate_go; generate_envoy ;;
  *) echo "unknown target: $TARGET" >&2; exit 1 ;;
esac
