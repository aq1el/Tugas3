from __future__ import annotations

import asyncio
import statistics
from collections import defaultdict
from typing import Dict, List


class Metrics:
    def __init__(self) -> None:
        self._counters = defaultdict(int)
        self._latencies: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def record(self, name: str, latency_ms: float) -> None:
        async with self._lock:
            self._counters[name] += 1
            self._latencies[name].append(latency_ms)
            if len(self._latencies[name]) > 1000:
                self._latencies[name] = self._latencies[name][-1000:]

    async def increment(self, name: str) -> None:
        async with self._lock:
            self._counters[name] += 1

    async def snapshot(self) -> Dict[str, object]:
        async with self._lock:
            counters = dict(self._counters)
            latencies = {key: list(values) for key, values in self._latencies.items()}

        stats = {}
        for key, values in latencies.items():
            if not values:
                continue
            values_sorted = sorted(values)
            p50 = statistics.median(values_sorted)
            p95_index = int(round(0.95 * (len(values_sorted) - 1)))
            p95 = values_sorted[p95_index]
            stats[key] = {"p50": p50, "p95": p95, "count": len(values_sorted)}
        return {"counters": counters, "latencies": stats}
