from __future__ import annotations

import asyncio
from typing import Any, Dict

from .utils import now_ms


class LockManager:
    def __init__(self) -> None:
        self._locks: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def apply_entry(self, command: Dict[str, Any]) -> Dict[str, Any]:
        op = command.get("op")
        resource = command.get("resource")
        owner = command.get("owner")
        ttl_ms = int(command.get("ttl_ms", 5000))

        async with self._lock:
            if op == "lock.acquire":
                return self._acquire(resource, owner, ttl_ms)
            if op == "lock.release":
                return self._release(resource, owner)

        return {"ok": False, "error": "unknown_op"}

    def _acquire(self, resource: str, owner: str, ttl_ms: int) -> Dict[str, Any]:
        now = now_ms()
        current = self._locks.get(resource)
        if current and current.get("expires_at", 0) > now and current.get("owner") != owner:
            return {"ok": False, "error": "locked", "owner": current.get("owner")}

        expires_at = now + ttl_ms
        self._locks[resource] = {"owner": owner, "expires_at": expires_at}
        return {"ok": True, "resource": resource, "owner": owner, "expires_at": expires_at}

    def _release(self, resource: str, owner: str) -> Dict[str, Any]:
        current = self._locks.get(resource)
        if not current:
            return {"ok": False, "error": "not_found"}
        if current.get("owner") != owner:
            return {"ok": False, "error": "not_owner"}

        self._locks.pop(resource, None)
        return {"ok": True, "resource": resource}

    async def status(self, resource: str) -> Dict[str, Any]:
        async with self._lock:
            current = self._locks.get(resource)
            if not current:
                return {"resource": resource, "locked": False}
            if current.get("expires_at", 0) <= now_ms():
                self._locks.pop(resource, None)
                return {"resource": resource, "locked": False}
            return {"resource": resource, "locked": True, "owner": current.get("owner"), "expires_at": current.get("expires_at")}
