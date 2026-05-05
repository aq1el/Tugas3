from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from .cluster import ClusterClient
from .config import Settings
from .utils import majority_count

ApplyFn = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]


class RaftLikeConsensus:
    def __init__(self, settings: Settings, cluster: ClusterClient, apply_fn: ApplyFn) -> None:
        self.settings = settings
        self.cluster = cluster
        self.apply_fn = apply_fn
        self.term = 1
        self.log: list[Dict[str, Any]] = []
        self.commit_index = -1
        self._lock = asyncio.Lock()
        self._append_lock = asyncio.Lock()
        self._pending: Dict[str, asyncio.Future] = {}

    @property
    def is_leader(self) -> bool:
        return self.settings.node_id == self.settings.leader_id

    async def append_entry(self, command: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if not self.is_leader:
            return False, {"ok": False, "error": "not_leader"}

        async with self._append_lock:
            return await self._append_entry_locked(command)

    async def _append_entry_locked(self, command: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if not self.is_leader:
            return False, {"ok": False, "error": "not_leader"}

        entry_id = command.get("id")
        future: Optional[asyncio.Future] = None
        if entry_id:
            future = asyncio.get_event_loop().create_future()
            self._pending[entry_id] = future

        async with self._lock:
            entry = {"term": self.term, "index": len(self.log), "command": command}
            self.log.append(entry)

        total_nodes = len(self.settings.peers) + 1
        required = majority_count(total_nodes)
        acks = 1
        request = {
            "term": self.term,
            "leader_id": self.settings.node_id,
            "entry": entry,
            "leader_commit": entry["index"],
        }

        tasks = [
            self.cluster.post_json(f"{peer}/internal/append", request, encrypt=True)
            for peer in self.settings.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("ok"):
                acks += 1

        if acks < required:
            return False, {"ok": False, "error": "quorum_failed", "acks": acks}

        await self._commit_up_to(entry["index"])
        if future:
            return True, await future
        return True, {"ok": True}

    async def handle_append(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        term = payload.get("term", 0)
        if term < self.term:
            return {"ok": False, "error": "stale_term"}

        entry = payload.get("entry")
        leader_commit = payload.get("leader_commit", -1)
        if entry is None:
            return {"ok": False, "error": "missing_entry"}

        async with self._lock:
            self.term = term
            index = entry.get("index", len(self.log))
            if index == len(self.log):
                self.log.append(entry)
            elif index < len(self.log):
                self.log[index] = entry
            else:
                return {"ok": False, "error": "log_gap"}

        commit_to = min(leader_commit, len(self.log) - 1)
        if commit_to >= 0:
            await self._commit_up_to(commit_to)
        return {"ok": True}

    async def _commit_up_to(self, index: int) -> None:
        while self.commit_index < index:
            self.commit_index += 1
            entry = self.log[self.commit_index]
            result = await self.apply_fn(entry["command"])
            entry_id = entry["command"].get("id")
            future = self._pending.pop(entry_id, None)
            if future and not future.done():
                future.set_result(result)
