from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

from .cluster import ClusterClient
from .config import Settings
from .utils import ConsistentHashRing, new_id, now_ms


class DistributedQueue:
    def __init__(self, settings: Settings, cluster: ClusterClient) -> None:
        self.settings = settings
        self.cluster = cluster
        self._queues: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        ring_nodes = [settings.node_url] + settings.peers
        self._ring = ConsistentHashRing(ring_nodes)

    def _target(self, queue_name: str, key: Optional[str]) -> Optional[str]:
        ring_key = key or queue_name
        return self._ring.get_node(ring_key)

    async def enqueue(self, queue_name: str, payload: Any, key: Optional[str] = None) -> Dict[str, Any]:
        target = self._target(queue_name, key)
        if not target or target == self.settings.node_url:
            return await self._enqueue_local(queue_name, payload)
        return await self.cluster.post_json(
            f"{target}/internal/queue/enqueue",
            {"queue": queue_name, "payload": payload},
            encrypt=True,
        )

    async def dequeue(self, queue_name: str, consumer: str) -> Dict[str, Any]:
        target = self._target(queue_name, consumer)
        if not target or target == self.settings.node_url:
            return await self._dequeue_local(queue_name, consumer)
        return await self.cluster.post_json(
            f"{target}/internal/queue/dequeue",
            {"queue": queue_name, "consumer": consumer},
            encrypt=True,
        )

    async def _enqueue_local(self, queue_name: str, payload: Any) -> Dict[str, Any]:
        async with self._lock:
            item = {"id": new_id(), "payload": payload, "ts": now_ms()}
            self._queues[queue_name].append(item)
            return {"ok": True, "queue": queue_name, "item": item}

    async def _dequeue_local(self, queue_name: str, consumer: str) -> Dict[str, Any]:
        async with self._lock:
            if not self._queues[queue_name]:
                return {"ok": False, "queue": queue_name, "item": None}
            item = self._queues[queue_name].popleft()
            return {"ok": True, "queue": queue_name, "item": item, "consumer": consumer}
