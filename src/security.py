from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException


ROLE_RANK = {"reader": 1, "writer": 2, "admin": 3}


def verify_api_key(provided: Optional[str], expected: Optional[str]) -> None:
    if expected and provided != expected:
        raise HTTPException(status_code=401, detail="invalid api key")


def resolve_role(provided: Optional[str], key_roles: Dict[str, str]) -> str:
    if not key_roles:
        raise HTTPException(status_code=401, detail="api key not configured")
    for role, key in key_roles.items():
        if provided == key:
            return role
    raise HTTPException(status_code=401, detail="invalid api key")


def require_role(provided: Optional[str], key_roles: Dict[str, str], min_role: str) -> str:
    role = resolve_role(provided, key_roles)
    if ROLE_RANK.get(role, 0) < ROLE_RANK.get(min_role, 0):
        raise HTTPException(status_code=403, detail="insufficient role")
    return role


def sign_body(body: bytes, key: bytes) -> str:
    return hmac.new(key, body, hashlib.sha256).hexdigest()


def verify_signature(body: bytes, signature: Optional[str], key: Optional[bytes]) -> None:
    if not key:
        return
    if not signature:
        raise HTTPException(status_code=401, detail="missing cluster signature")
    expected = sign_body(body, key)
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="invalid cluster signature")


def encrypt_json(payload: Dict[str, Any], key: bytes) -> Dict[str, str]:
    raw = json.dumps(payload).encode("utf-8")
    nonce = os.urandom(12)
    cipher = AESGCM(key).encrypt(nonce, raw, None)
    return {
        "nonce": base64.b64encode(nonce).decode("utf-8"),
        "ciphertext": base64.b64encode(cipher).decode("utf-8"),
    }


def decrypt_json(payload: Dict[str, str], key: bytes) -> Dict[str, Any]:
    nonce_b64 = payload.get("nonce", "")
    cipher_b64 = payload.get("ciphertext", "")
    nonce = base64.b64decode(nonce_b64)
    cipher = base64.b64decode(cipher_b64)
    raw = AESGCM(key).decrypt(nonce, cipher, None)
    return json.loads(raw.decode("utf-8"))
