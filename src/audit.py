from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AuditRecord:
    event: str
    actor: str
    data: Dict[str, Any]
    prev_hash: str
    hash: str


class AuditLogger:
    def __init__(self, path: str, key: Optional[bytes]) -> None:
        self.path = path
        self.key = key
        self._lock = asyncio.Lock()
        self._last_hash = "0" * 64
        os.makedirs(os.path.dirname(path), exist_ok=True)

    async def log(self, event: str, actor: str, data: Dict[str, Any]) -> None:
        if not self.key:
            return
        async with self._lock:
            record = {
                "event": event,
                "actor": actor,
                "data": data,
                "prev_hash": self._last_hash,
            }
            payload = json.dumps(record, sort_keys=True)
            digest = hmac.new(self.key, payload.encode("utf-8"), hashlib.sha256).hexdigest()
            record["hash"] = digest
            await asyncio.to_thread(self._append_line, record)
            self._last_hash = digest

    def _append_line(self, record: Dict[str, Any]) -> None:
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
