# Build context: repository root.
FROM python:3.13-slim AS descriptors
WORKDIR /build
RUN pip install --no-cache-dir grpcio-tools==1.84.1
COPY proto/ proto/
RUN python -m grpc_tools.protoc --proto_path=proto --proto_path=proto/third_party \
      --include_imports --descriptor_set_out=descriptors.pb \
      proto/shop/order/v1/order.proto proto/shop/inventory/v1/inventory.proto

FROM envoyproxy/envoy:v1.38-latest
COPY envoy/envoy.yaml /etc/envoy/envoy.yaml
COPY --from=descriptors /build/descriptors.pb /etc/envoy/descriptors.pb
