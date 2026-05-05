from __future__ import annotations

import asyncio

from src.nodes.cache_node import CacheNode
from src.utils.config import Settings


def _settings(policy: str) -> Settings:
    return Settings(
        node_id="node",
        node_host="0.0.0.0",
        node_port=8000,
        node_url="http://node",
        peers=[],
        leader_id="node",
        leader_url="http://node",
        api_key="devkey",
        api_key_roles={"admin": "devkey"},
        cluster_hmac_key=None,
        encryption_key=None,
        ml_enabled=False,
        request_timeout=1.0,
        redis_url=None,
        queue_visibility_timeout=30.0,
        queue_reclaim_interval=5.0,
        cache_max_items=2,
        cache_policy=policy,
        audit_log_key=None,
        audit_log_path="logs/audit.log",
        blocked_peers=[],
        use_peer_registry=False,
    )


def test_lru_eviction() -> None:
    cache = CacheNode(_settings("LRU"))
    asyncio.run(cache.set_entry("a", 1, "S"))
    asyncio.run(cache.set_entry("b", 2, "S"))
    asyncio.run(cache.read_local("a"))
    asyncio.run(cache.set_entry("c", 3, "S"))
    assert len(cache._entries) <= 2
    assert "a" in cache._entries


def test_lfu_eviction() -> None:
    cache = CacheNode(_settings("LFU"))
    asyncio.run(cache.set_entry("a", 1, "S"))
    asyncio.run(cache.set_entry("b", 2, "S"))
    asyncio.run(cache.read_local("a"))
    asyncio.run(cache.read_local("a"))
    asyncio.run(cache.set_entry("c", 3, "S"))
    assert len(cache._entries) <= 2
    assert "a" in cache._entries
