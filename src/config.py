from __future__ import annotations

from dataclasses import dataclass
import base64
import os
from typing import List, Optional


def _get_list(name: str) -> List[str]:
    raw = os.getenv(name, "")
    return [value.strip() for value in raw.split(",") if value.strip()]


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_b64_bytes(name: str) -> Optional[bytes]:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        return base64.b64decode(raw)
    except Exception:
        return None


@dataclass(frozen=True)
class Settings:
    node_id: str
    node_host: str
    node_port: int
    node_url: str
    peers: List[str]
    leader_id: str
    leader_url: str
    api_key: Optional[str]
    cluster_hmac_key: Optional[bytes]
    encryption_key: Optional[bytes]
    ml_enabled: bool
    request_timeout: float


def load_settings() -> Settings:
    node_id = os.getenv("NODE_ID", "node-1")
    node_host = os.getenv("NODE_HOST", "0.0.0.0")
    node_port = _get_int("NODE_PORT", 8000)
    node_url = os.getenv("NODE_URL", f"http://{node_host}:{node_port}")
    peers = _get_list("PEERS")
    peers = [peer for peer in peers if peer != node_url]
    leader_id = os.getenv("LEADER_ID", node_id)
    leader_url = os.getenv("LEADER_URL", node_url)
    api_key = os.getenv("API_KEY")
    cluster_hmac_key = os.getenv("CLUSTER_HMAC_KEY")
    cluster_hmac_bytes = cluster_hmac_key.encode("utf-8") if cluster_hmac_key else None
    encryption_key = _get_b64_bytes("ENCRYPTION_KEY")
    ml_enabled = _get_bool("ML_ENABLED", False)
    request_timeout = _get_float("REQUEST_TIMEOUT", 3.5)

    return Settings(
        node_id=node_id,
        node_host=node_host,
        node_port=node_port,
        node_url=node_url,
        peers=peers,
        leader_id=leader_id,
        leader_url=leader_url,
        api_key=api_key,
        cluster_hmac_key=cluster_hmac_bytes,
        encryption_key=encryption_key,
        ml_enabled=ml_enabled,
        request_timeout=request_timeout,
    )
