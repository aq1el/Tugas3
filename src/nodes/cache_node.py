from __future__ import annotations

import asyncio
from dataclasses import dataclass
from collections import OrderedDict
from typing import Any, Dict, Optional, Set

from ..communication.message_passing import ClusterClient
from ..utils.config import Settings
from ..utils.helpers import now_ms


@dataclass
class CacheEntry:
    value: Any
    state: str
    version: int


class CacheNode:
    def __init__(self, settings: Settings) -> None:
        self.node_id = settings.node_id
        self._max_items = settings.cache_max_items
        self._policy = settings.cache_policy.upper()
        self._entries: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._lru = OrderedDict()
        self._counts: Dict[str, int] = {}

    async def read_local(self, key: str) -> Optional[CacheEntry]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry or entry.state == "I":
                return None
            self._record_access(key)
            return entry

    async def set_entry(self, key: str, value: Any, state: str) -> None:
        async with self._lock:
            current = self._entries.get(key)
            version = (current.version + 1) if current else 1
            self._entries[key] = CacheEntry(value=value, state=state, version=version)
            self._record_access(key)
            self._evict_if_needed()

    async def invalidate(self, key: str) -> None:
        async with self._lock:
            entry = self._entries.get(key)
            if entry:
                entry.state = "I"

    async def writeback(self, key: str) -> Optional[Any]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return None
            entry.state = "S"
            self._record_access(key)
            return entry.value

    def _record_access(self, key: str) -> None:
        if self._policy == "LFU":
            self._counts[key] = self._counts.get(key, 0) + 1
            return
        self._lru.pop(key, None)
        self._lru[key] = now_ms()

    def _evict_if_needed(self) -> None:
        if self._max_items <= 0 or len(self._entries) <= self._max_items:
            return
        if self._policy == "LFU" and self._counts:
            evict_key = min(self._counts, key=self._counts.get)
        else:
            evict_key = next(iter(self._lru)) if self._lru else None
        if not evict_key:
            return
        self._entries.pop(evict_key, None)
        self._lru.pop(evict_key, None)
        self._counts.pop(evict_key, None)


class CacheCoordinator:
    def __init__(self, settings: Settings, cluster: ClusterClient) -> None:
        self.settings = settings
        self.cluster = cluster
        self._directory: Dict[str, Dict[str, Any]] = {}
        self._store: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def handle_read(self, node_id: str, node_url: str, key: str) -> Dict[str, Any]:
        async with self._lock:
            entry = self._directory.get(key)
            if not entry:
                value = self._store.get(key)
                self._directory[key] = {
                    "state": "E",
                    "owner": node_url,
                    "sharers": {node_url},
                }
                return {"value": value, "state": "E"}

            state = entry["state"]
            if state == "M":
                owner = entry.get("owner")
                if owner and owner != node_url:
                    response = await self.cluster.post_json(
                        f"{owner}/internal/cache/writeback",
                        {"key": key},
                        encrypt=True,
                    )
                    value = response.get("value")
                    self._store[key] = value
                    entry["state"] = "S"
                    entry["owner"] = None
                    entry["sharers"] = {owner, node_url}
                    return {"value": value, "state": "S"}

            if state in ("E", "S"):
                entry["sharers"].add(node_url)
                if state == "E" and len(entry["sharers"]) > 1:
                    entry["state"] = "S"
                    entry["owner"] = None
                return {"value": self._store.get(key), "state": entry["state"]}

        return {"value": None, "state": "I"}

    async def handle_write(self, node_id: str, node_url: str, key: str, value: Any) -> Dict[str, Any]:
        async with self._lock:
            entry = self._directory.get(key)
            targets: Set[str] = set()
            if entry:
                targets.update(entry.get("sharers", set()))
                owner = entry.get("owner")
                if owner:
                    targets.add(owner)
            targets.discard(node_url)

            for target in targets:
                await self.cluster.post_json(
                    f"{target}/internal/cache/invalidate",
                    {"key": key},
                    encrypt=True,
                )

            self._directory[key] = {
                "state": "M",
                "owner": node_url,
                "sharers": {node_url},
            }
            self._store[key] = value
            return {"ok": True, "state": "M"}
