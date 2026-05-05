from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import httpx
import redis.asyncio as redis

from ..utils.config import Settings
from ..security import encrypt_json, sign_body


class ClusterClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(timeout=settings.request_timeout)
        self._redis = None
        if settings.redis_url and settings.use_peer_registry:
            self._redis = redis.from_url(settings.redis_url, decode_responses=True)

    async def close(self) -> None:
        await self.client.aclose()
        if self._redis:
            await self._redis.close()

    async def register_node(self) -> None:
        if not self._redis:
            return
        await self._redis.sadd("cluster:nodes", self.settings.node_url)

    async def peer_urls(self) -> List[str]:
        if not self._redis:
            return list(self.settings.peers)
        nodes = await self._redis.smembers("cluster:nodes")
        return [node for node in nodes if node != self.settings.node_url]

    def leader_url(self) -> str:
        return self.settings.leader_url or self.settings.node_url

    def node_urls(self) -> list[str]:
        urls = [self.settings.node_url]
        urls.extend(self.settings.peers)
        return urls

    async def post_json(self, url: str, payload: Dict[str, Any], encrypt: bool = False) -> Dict[str, Any]:
        if any(url.startswith(peer) for peer in self.settings.blocked_peers):
            raise httpx.ConnectError("partitioned")
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
