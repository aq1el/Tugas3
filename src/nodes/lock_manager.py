from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Set

from ..utils.helpers import now_ms


@dataclass
class LockEntry:
    mode: str
    owners: Dict[str, int]


class LockManager:
    def __init__(self) -> None:
        self._locks: Dict[str, LockEntry] = {}
        self._wait_for: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()

    async def apply_entry(self, command: Dict[str, Any]) -> Dict[str, Any]:
        op = command.get("op")
        resource = command.get("resource")
        owner = command.get("owner")
        ttl_ms = int(command.get("ttl_ms", 5000))
        mode = command.get("mode", "exclusive")

        async with self._lock:
            if op == "lock.acquire":
                return self._acquire(resource, owner, ttl_ms, mode)
            if op == "lock.release":
                return self._release(resource, owner)

        return {"ok": False, "error": "unknown_op"}

    def _acquire(self, resource: str, owner: str, ttl_ms: int, mode: str) -> Dict[str, Any]:
        mode = mode.lower()
        if mode not in ("shared", "exclusive"):
            return {"ok": False, "error": "invalid_mode"}

        self._cleanup_expired(resource)
        entry = self._locks.get(resource)
        expires_at = now_ms() + ttl_ms

        if not entry:
            new_mode = "S" if mode == "shared" else "X"
            self._locks[resource] = LockEntry(mode=new_mode, owners={owner: expires_at})
            self._clear_waits(owner)
            return {
                "ok": True,
                "resource": resource,
                "owner": owner,
                "mode": new_mode,
                "expires_at": expires_at,
            }

        if mode == "shared":
            if entry.mode == "S" or owner in entry.owners:
                entry.owners[owner] = expires_at
                self._clear_waits(owner)
                return {
                    "ok": True,
                    "resource": resource,
                    "owner": owner,
                    "mode": entry.mode,
                    "expires_at": expires_at,
                }
            return self._wait_or_deadlock(resource, owner, entry)

        if mode == "exclusive":
            if entry.mode == "X" and owner in entry.owners:
                entry.owners[owner] = expires_at
                self._clear_waits(owner)
                return {
                    "ok": True,
                    "resource": resource,
                    "owner": owner,
                    "mode": "X",
                    "expires_at": expires_at,
                }
            if entry.mode == "S" and owner in entry.owners and len(entry.owners) == 1:
                entry.mode = "X"
                entry.owners[owner] = expires_at
                self._clear_waits(owner)
                return {
                    "ok": True,
                    "resource": resource,
                    "owner": owner,
                    "mode": "X",
                    "expires_at": expires_at,
                }
            if entry.mode == "S" and not entry.owners:
                entry.mode = "X"
                entry.owners[owner] = expires_at
                self._clear_waits(owner)
                return {
                    "ok": True,
                    "resource": resource,
                    "owner": owner,
                    "mode": "X",
                    "expires_at": expires_at,
                }
            return self._wait_or_deadlock(resource, owner, entry)

        return {"ok": False, "error": "invalid_mode"}

    def _wait_or_deadlock(self, resource: str, owner: str, entry: LockEntry) -> Dict[str, Any]:
        holders = set(entry.owners.keys())
        self._wait_for[owner] = holders
        if self._detect_deadlock(owner):
            self._wait_for.pop(owner, None)
            return {"ok": False, "error": "deadlock_detected", "resource": resource}
        return {"ok": False, "error": "locked", "resource": resource, "holders": list(holders)}

    def _release(self, resource: str, owner: str) -> Dict[str, Any]:
        entry = self._locks.get(resource)
        if not entry:
            return {"ok": False, "error": "not_found"}
        if owner not in entry.owners:
            return {"ok": False, "error": "not_owner"}

        entry.owners.pop(owner, None)
        if not entry.owners:
            self._locks.pop(resource, None)
        self._remove_owner_from_waits(owner)
        return {"ok": True, "resource": resource}

    def _cleanup_expired(self, resource: str) -> None:
        entry = self._locks.get(resource)
        if not entry:
            return
        now = now_ms()
        expired = [owner for owner, expires_at in entry.owners.items() if expires_at <= now]
        for owner in expired:
            entry.owners.pop(owner, None)
        if not entry.owners:
            self._locks.pop(resource, None)

    def _clear_waits(self, owner: str) -> None:
        self._wait_for.pop(owner, None)

    def _remove_owner_from_waits(self, owner: str) -> None:
        for waiting, holders in list(self._wait_for.items()):
            if owner in holders:
                holders.discard(owner)
            if not holders:
                self._wait_for.pop(waiting, None)

    def _detect_deadlock(self, start_owner: str) -> bool:
        visited: Set[str] = set()
        stack: Set[str] = set()

        def visit(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            stack.add(node)
            for dep in self._wait_for.get(node, set()):
                if visit(dep):
                    return True
            stack.remove(node)
            return False

        return visit(start_owner)

    async def status(self, resource: str) -> Dict[str, Any]:
        async with self._lock:
            self._cleanup_expired(resource)
            entry = self._locks.get(resource)
            if not entry:
                return {"resource": resource, "locked": False}
            return {
                "resource": resource,
                "locked": True,
                "mode": entry.mode,
                "owners": list(entry.owners.keys()),
            }
