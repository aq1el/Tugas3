from __future__ import annotations

import asyncio

from src.communication.message_passing import ClusterClient
from src.nodes.queue_node import DistributedQueue
from src.utils.config import Settings


def _settings() -> Settings:
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
        cache_max_items=10,
        cache_policy="LRU",
        audit_log_key=None,
        audit_log_path="logs/audit.log",
        blocked_peers=[],
        use_peer_registry=False,
    )


def test_queue_in_memory() -> None:
    settings = _settings()
    queue = DistributedQueue(settings, ClusterClient(settings))
    result = asyncio.run(queue._enqueue_local("jobs", {"task": "x"}))
    assert result["ok"]
    result = asyncio.run(queue._dequeue_local("jobs", "worker"))
    assert result["ok"]
