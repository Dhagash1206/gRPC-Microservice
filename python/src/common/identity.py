"""End-user claims forwarded by Envoy after JWT validation (x-jwt-payload).

Trustworthy only on calls whose mTLS peer is Envoy, which overwrites the header
with the verified token's claims; callers must check the peer first.
"""

import base64
import binascii
import json

import grpc

JWT_PAYLOAD_METADATA_KEY = "x-jwt-payload"


def forwarded_claims(context: grpc.ServicerContext) -> dict | None:
    values = [value for key, value in context.invocation_metadata() if key == JWT_PAYLOAD_METADATA_KEY]
    if len(values) != 1:
        return None
    encoded_payload = values[0]
    try:
        claims = json.loads(base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4)))
    except (binascii.Error, ValueError):
        return None
    return claims if isinstance(claims, dict) else None


def has_role(claims: dict | None, role: str) -> bool:
    roles = (claims or {}).get("roles")
    return isinstance(roles, list) and role in roles
