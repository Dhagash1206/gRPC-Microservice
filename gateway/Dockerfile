# Build context: repository root.
FROM python:3.13-slim AS descriptors
WORKDIR /build
RUN pip install --no-cache-dir grpcio-tools==1.84.1
COPY api/ api/
RUN python -m grpc_tools.protoc --proto_path=api --proto_path=api/third_party \
      --include_imports --descriptor_set_out=descriptors.pb \
      api/shop/order/v1/order.proto api/shop/inventory/v1/inventory.proto

FROM envoyproxy/envoy:v1.38-latest
COPY gateway/envoy.yaml /etc/envoy/envoy.yaml
COPY --from=descriptors /build/descriptors.pb /etc/envoy/descriptors.pb
