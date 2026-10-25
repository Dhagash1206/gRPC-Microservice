"""Container health probe over mTLS using the container's own cert.

python -m common.healthcheck   (reads PORT and TLS_* env; exit 0 = SERVING)
"""

import os
import sys

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from common.tls import TlsFiles, channel_credentials


def main() -> int:
    target = f"localhost:{os.environ['PORT']}"
    try:
        with grpc.secure_channel(target, channel_credentials(TlsFiles.from_env())) as channel:
            response = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(), timeout=2)
    except grpc.RpcError:
        return 1
    return 0 if response.status == health_pb2.HealthCheckResponse.SERVING else 1


if __name__ == "__main__":
    sys.exit(main())
