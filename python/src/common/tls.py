"""mTLS credentials. Every service presents its own cert and requires one from callers."""

import os
import pathlib
from dataclasses import dataclass

import grpc


@dataclass(frozen=True)
class TlsFiles:
    cert_file: pathlib.Path
    key_file: pathlib.Path
    ca_file: pathlib.Path

    @classmethod
    def from_env(cls) -> "TlsFiles":
        return cls(
            cert_file=pathlib.Path(os.environ["TLS_CERT_FILE"]),
            key_file=pathlib.Path(os.environ["TLS_KEY_FILE"]),
            ca_file=pathlib.Path(os.environ["TLS_CA_FILE"]),
        )

    @classmethod
    def for_workload(cls, certs_dir: pathlib.Path, workload_name: str) -> "TlsFiles":
        return cls(
            cert_file=certs_dir / f"{workload_name}.crt",
            key_file=certs_dir / f"{workload_name}.key",
            ca_file=certs_dir / "ca.crt",
        )


def server_credentials(tls_files: TlsFiles) -> grpc.ServerCredentials:
    return grpc.ssl_server_credentials(
        [(tls_files.key_file.read_bytes(), tls_files.cert_file.read_bytes())],
        root_certificates=tls_files.ca_file.read_bytes(),
        require_client_auth=True,
    )


def channel_credentials(tls_files: TlsFiles) -> grpc.ChannelCredentials:
    return grpc.ssl_channel_credentials(
        root_certificates=tls_files.ca_file.read_bytes(),
        private_key=tls_files.key_file.read_bytes(),
        certificate_chain=tls_files.cert_file.read_bytes(),
    )
