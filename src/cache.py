from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from .cluster import ClusterClient
from .config import Settings


@dataclass
class CacheEntry:
    value: Any
    state: str
    version: int


class CacheNode:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._entries: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def read_local(self, key: str) -> Optional[CacheEntry]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry or entry.state == "I":
                return None
            return entry

    async def set_entry(self, key: str, value: Any, state: str) -> None:
        async with self._lock:
            current = self._entries.get(key)
            version = (current.version + 1) if current else 1
            self._entries[key] = CacheEntry(value=value, state=state, version=version)

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
            return entry.value


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
