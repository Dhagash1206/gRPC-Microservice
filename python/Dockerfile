# Build context: repository root. One image serves both inventory and payment.
FROM python:3.13-slim AS stubs
WORKDIR /build
RUN pip install --no-cache-dir grpcio-tools==1.84.1
COPY proto/ proto/
RUN mkdir out && python -m grpc_tools.protoc --proto_path=proto --proto_path=proto/third_party \
      --python_out=out --grpc_python_out=out proto/shop/*/v1/*.proto \
    && find out/shop -type d -exec touch {}/__init__.py \;

FROM python:3.13-slim
WORKDIR /app
COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --no-create-home app
COPY python/src/ src/
COPY --from=stubs /build/out/shop src/shop
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
USER app
