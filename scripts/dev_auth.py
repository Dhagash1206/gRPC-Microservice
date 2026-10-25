"""Dev identity provider.

  dev_auth.py jwks  --public-key certs/jwt_public.pem          -> JWKS JSON for Envoy
  dev_auth.py token --sub customer-42 [--ttl-minutes 60] [--role admin]  -> signed RS256 JWT
"""

import argparse
import base64
import json
import pathlib
import time

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_public_key

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
ISSUER = "https://auth.grpcshop.local"
AUDIENCE = "grpcshop-api"
KEY_ID = "dev-key-1"


def base64url_uint(value: int) -> str:
    value_bytes = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(value_bytes).rstrip(b"=").decode()


def build_jwks(public_key_path: pathlib.Path) -> dict:
    public_numbers = load_pem_public_key(public_key_path.read_bytes()).public_numbers()
    return {
        "keys": [
            {
                "kty": "RSA",
                "alg": "RS256",
                "use": "sig",
                "kid": KEY_ID,
                "n": base64url_uint(public_numbers.n),
                "e": base64url_uint(public_numbers.e),
            }
        ]
    }


def issue_token(private_key_path: pathlib.Path, subject: str, ttl_minutes: int, roles: list[str] | None = None) -> str:
    issued_at = int(time.time())
    claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": subject, "iat": issued_at, "exp": issued_at + ttl_minutes * 60}
    if roles:
        claims["roles"] = roles
    return jwt.encode(claims, private_key_path.read_bytes(), algorithm="RS256", headers={"kid": KEY_ID})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest="command", required=True)

    jwks_parser = subcommands.add_parser("jwks")
    jwks_parser.add_argument("--public-key", type=pathlib.Path, default=PROJECT_ROOT / "certs/jwt_public.pem")

    token_parser = subcommands.add_parser("token")
    token_parser.add_argument("--sub", required=True, help="customer id")
    token_parser.add_argument("--ttl-minutes", type=int, default=60)
    token_parser.add_argument("--role", action="append", dest="roles", help="repeatable, e.g. --role admin")
    token_parser.add_argument("--private-key", type=pathlib.Path, default=PROJECT_ROOT / "certs/jwt_private.pem")

    args = parser.parse_args()
    if args.command == "jwks":
        print(json.dumps(build_jwks(args.public_key), indent=2))
    else:
        print(issue_token(args.private_key, args.sub, args.ttl_minutes, args.roles))


if __name__ == "__main__":
    main()
