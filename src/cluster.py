from __future__ import annotations

import json
from typing import Any, Dict, Optional

import httpx

from .config import Settings
from .security import encrypt_json, sign_body


class ClusterClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(timeout=settings.request_timeout)

    async def close(self) -> None:
        await self.client.aclose()

    def leader_url(self) -> str:
        return self.settings.leader_url or self.settings.node_url

    def node_urls(self) -> list[str]:
        urls = [self.settings.node_url]
        urls.extend(self.settings.peers)
        return urls

    async def post_json(self, url: str, payload: Dict[str, Any], encrypt: bool = False) -> Dict[str, Any]:
        body = payload
        headers = {"X-Node-Id": self.settings.node_id}
        if encrypt and self.settings.encryption_key:
            body = encrypt_json(payload, self.settings.encryption_key)
            headers["X-Cluster-Encrypted"] = "1"
        raw = json.dumps(body).encode("utf-8")
        if self.settings.cluster_hmac_key:
            headers["X-Cluster-Signature"] = sign_body(raw, self.settings.cluster_hmac_key)
        response = await self.client.post(url, content=raw, headers=headers)
        response.raise_for_status()
        return response.json()
