from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

import redis.asyncio as redis

from ..communication.message_passing import ClusterClient
from ..utils.config import Settings
from ..utils.helpers import ConsistentHashRing, new_id, now_ms


class DistributedQueue:
    def __init__(self, settings: Settings, cluster: ClusterClient) -> None:
        self.settings = settings
        self.cluster = cluster
        self._queues: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        ring_nodes = [settings.node_url] + settings.peers
        self._ring = ConsistentHashRing(ring_nodes)
        self._redis = redis.from_url(settings.redis_url, decode_responses=True) if settings.redis_url else None
        self._reclaim_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._redis:
            self._reclaim_task = asyncio.create_task(self._reclaim_loop())

    async def close(self) -> None:
        if self._reclaim_task:
            self._reclaim_task.cancel()
        if self._redis:
            await self._redis.close()

    def _target(self, queue_name: str, key: Optional[str]) -> Optional[str]:
        ring_key = key or queue_name
        return self._ring.get_node(ring_key)

    async def _refresh_ring(self) -> None:
        if not self.settings.use_peer_registry:
            return
        peers = await self.cluster.peer_urls()
        ring_nodes = [self.settings.node_url] + peers
        self._ring = ConsistentHashRing(ring_nodes)

    async def enqueue(self, queue_name: str, payload: Any, key: Optional[str] = None) -> Dict[str, Any]:
        await self._refresh_ring()
        target = self._target(queue_name, key)
        if not target or target == self.settings.node_url:
            return await self._enqueue_local(queue_name, payload)
        return await self.cluster.post_json(
            f"{target}/internal/queue/enqueue",
            {"queue": queue_name, "payload": payload},
            encrypt=True,
        )

    async def dequeue(self, queue_name: str, consumer: str) -> Dict[str, Any]:
        await self._refresh_ring()
        target = self._target(queue_name, consumer)
        if not target or target == self.settings.node_url:
            return await self._dequeue_local(queue_name, consumer)
        return await self.cluster.post_json(
            f"{target}/internal/queue/dequeue",
            {"queue": queue_name, "consumer": consumer},
            encrypt=True,
        )

    async def ack(self, queue_name: str, receipt_id: str) -> Dict[str, Any]:
        await self._refresh_ring()
        target = self._target(queue_name, queue_name)
        if not target or target == self.settings.node_url:
            return await self._ack_local(queue_name, receipt_id)
        return await self.cluster.post_json(
            f"{target}/internal/queue/ack",
            {"queue": queue_name, "receipt_id": receipt_id},
            encrypt=True,
        )

    def _ready_key(self, queue_name: str) -> str:
        return f"queue:{queue_name}:ready"

    def _payload_key(self, queue_name: str) -> str:
        return f"queue:{queue_name}:payload"

    def _inflight_key(self, queue_name: str) -> str:
        return f"queue:{queue_name}:inflight"

    def _queue_registry_key(self) -> str:
        return "queue:names"

    def _reclaim_lock_key(self, queue_name: str) -> str:
        return f"queue:{queue_name}:reclaim_lock"

    async def _enqueue_local(self, queue_name: str, payload: Any) -> Dict[str, Any]:
        if not self._redis:
            async with self._lock:
                item = {"id": new_id(), "payload": payload, "ts": now_ms()}
                self._queues[queue_name].append(item)
                return {"ok": True, "queue": queue_name, "item": item}

        item_id = new_id()
        payload_raw = json.dumps(payload)
        await self._redis.hset(self._payload_key(queue_name), item_id, payload_raw)
        await self._redis.rpush(self._ready_key(queue_name), item_id)
        await self._redis.sadd(self._queue_registry_key(), queue_name)
        item = {"id": item_id, "payload": payload, "ts": now_ms()}
        return {"ok": True, "queue": queue_name, "item": item}

    async def _dequeue_local(self, queue_name: str, consumer: str) -> Dict[str, Any]:
        if not self._redis:
            async with self._lock:
                if not self._queues[queue_name]:
                    return {"ok": False, "queue": queue_name, "item": None}
                item = self._queues[queue_name].popleft()
                return {"ok": True, "queue": queue_name, "item": item, "consumer": consumer}

        item_id = await self._redis.rpop(self._ready_key(queue_name))
        if not item_id:
            return {"ok": False, "queue": queue_name, "item": None}
        payload_raw = await self._redis.hget(self._payload_key(queue_name), item_id)
        if payload_raw is None:
            return {"ok": False, "queue": queue_name, "item": None}

        deadline = now_ms() + int(self.settings.queue_visibility_timeout * 1000)
        await self._redis.zadd(self._inflight_key(queue_name), {item_id: deadline})
        payload = json.loads(payload_raw)
        item = {"id": item_id, "payload": payload, "ts": now_ms()}
        return {"ok": True, "queue": queue_name, "item": item, "consumer": consumer, "receipt_id": item_id}

    async def _ack_local(self, queue_name: str, receipt_id: str) -> Dict[str, Any]:
        if not self._redis:
            return {"ok": True, "queue": queue_name, "receipt_id": receipt_id}

        await self._redis.hdel(self._payload_key(queue_name), receipt_id)
        await self._redis.zrem(self._inflight_key(queue_name), receipt_id)
        await self._redis.lrem(self._ready_key(queue_name), 0, receipt_id)
        return {"ok": True, "queue": queue_name, "receipt_id": receipt_id}

    async def _reclaim_loop(self) -> None:
        assert self._redis is not None
        while True:
            await asyncio.sleep(self.settings.queue_reclaim_interval)
            queue_names = await self._redis.smembers(self._queue_registry_key())
            if not queue_names:
                continue
            for queue_name in queue_names:
                await self._reclaim_queue(queue_name)

    async def _reclaim_queue(self, queue_name: str) -> None:
        assert self._redis is not None
        lock_key = self._reclaim_lock_key(queue_name)
        acquired = await self._redis.set(lock_key, self.settings.node_id, nx=True, ex=2)
        if not acquired:
            return

        try:
            now = now_ms()
            inflight_key = self._inflight_key(queue_name)
            ready_key = self._ready_key(queue_name)
            due_ids = await self._redis.zrangebyscore(inflight_key, 0, now, start=0, num=100)
            if not due_ids:
                return
            for item_id in due_ids:
                await self._redis.rpush(ready_key, item_id)
                deadline = now + int(self.settings.queue_visibility_timeout * 1000)
                await self._redis.zadd(inflight_key, {item_id: deadline})
        finally:
            await self._redis.delete(lock_key)
