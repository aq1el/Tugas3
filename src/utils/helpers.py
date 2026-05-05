from __future__ import annotations

import hashlib
import time
import uuid
from bisect import bisect_left
from typing import Iterable, Optional


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id() -> str:
    return uuid.uuid4().hex


def majority_count(total_nodes: int) -> int:
    return (total_nodes // 2) + 1


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str], replicas: int = 64) -> None:
        self.replicas = replicas
        self._ring = {}
        self._sorted = []
        self._nodes = set()
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for index in range(self.replicas):
            key = f"{node}:{index}"
            hashed = self._hash(key)
            self._ring[hashed] = node
            self._sorted.append(hashed)
        self._sorted.sort()

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.remove(node)
        remove_keys = []
        for index in range(self.replicas):
            key = f"{node}:{index}"
            remove_keys.append(self._hash(key))
        for key in remove_keys:
            self._ring.pop(key, None)
        self._sorted = [key for key in self._sorted if key in self._ring]

    def get_node(self, key: str) -> Optional[str]:
        if not self._sorted:
            return None
        hashed = self._hash(key)
        idx = bisect_left(self._sorted, hashed)
        if idx == len(self._sorted):
            idx = 0
        return self._ring[self._sorted[idx]]
