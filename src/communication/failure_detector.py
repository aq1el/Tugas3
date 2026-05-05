from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict


@dataclass
class PeerStatus:
    failures: int = 0
    healthy: bool = True


class FailureDetector:
    def __init__(self) -> None:
        self._status: Dict[str, PeerStatus] = {}
        self._lock = asyncio.Lock()

    async def report_failure(self, peer: str) -> None:
        async with self._lock:
            status = self._status.setdefault(peer, PeerStatus())
            status.failures += 1
            status.healthy = False

    async def report_success(self, peer: str) -> None:
        async with self._lock:
            status = self._status.setdefault(peer, PeerStatus())
            status.failures = 0
            status.healthy = True

    async def snapshot(self) -> Dict[str, PeerStatus]:
        async with self._lock:
            return {peer: PeerStatus(failures=s.failures, healthy=s.healthy) for peer, s in self._status.items()}
