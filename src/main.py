from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from .nodes.cache_node import CacheCoordinator, CacheNode
from .communication.message_passing import ClusterClient
from .utils.config import load_settings
from .consensus.raft import RaftLikeConsensus
from .nodes.lock_manager import LockManager
from .utils.metrics import Metrics
from .ml import LoadPredictor
from .nodes.queue_node import DistributedQueue
from .audit import AuditLogger
from .security import decrypt_json, require_role, verify_signature
from .utils.helpers import new_id

settings = load_settings()
cluster = ClusterClient(settings)
metrics = Metrics()
lock_manager = LockManager()
consensus = RaftLikeConsensus(settings, cluster, lock_manager.apply_entry)
queue_manager = DistributedQueue(settings, cluster)
cache_node = CacheNode(settings)
cache_coordinator = CacheCoordinator(settings, cluster) if consensus.is_leader else None
predictor = LoadPredictor(enabled=settings.ml_enabled)
audit_logger = AuditLogger(settings.audit_log_path, settings.audit_log_key)

app = FastAPI(title="Tugas 3 - Distributed Systems Simulator")


class LockAcquireRequest(BaseModel):
    resource: str
    owner: str
    ttl_ms: int = 5000
    mode: str = "exclusive"


class LockReleaseRequest(BaseModel):
    resource: str
    owner: str


class QueueEnqueueRequest(BaseModel):
    queue: str
    payload: Any
    key: Optional[str] = None


class QueueDequeueRequest(BaseModel):
    queue: str
    consumer: str


class QueueAckRequest(BaseModel):
    queue: str
    receipt_id: str


class CacheReadRequest(BaseModel):
    key: str


class CacheWriteRequest(BaseModel):
    key: str
    value: Any


async def parse_cluster_request(request: Request) -> Dict[str, Any]:
    raw = await request.body()
    verify_signature(raw, request.headers.get("X-Cluster-Signature"), settings.cluster_hmac_key)
    if request.headers.get("X-Cluster-Encrypted") == "1" and settings.encryption_key:
        body = json.loads(raw.decode("utf-8"))
        return decrypt_json(body, settings.encryption_key)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    await metrics.record("http_request_ms", elapsed_ms)
    await metrics.increment("http_requests_total")
    return response


@app.on_event("startup")
async def start_background_tasks() -> None:
    async def sampler() -> None:
        last_total = 0
        while True:
            await asyncio.sleep(5)
            snapshot = await metrics.snapshot()
            total = snapshot["counters"].get("http_requests_total", 0)
            rps = (total - last_total) / 5.0
            predictor.add_sample(rps)
            last_total = total

    asyncio.create_task(sampler())
    await cluster.register_node()
    await queue_manager.start()


@app.on_event("shutdown")
async def shutdown_cluster() -> None:
    await cluster.close()
    await queue_manager.close()


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "node_id": settings.node_id,
        "leader": consensus.is_leader,
    }


@app.post("/lock/acquire")
async def lock_acquire(
    payload: LockAcquireRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    command = {
        "id": new_id(),
        "op": "lock.acquire",
        "resource": payload.resource,
        "owner": payload.owner,
        "ttl_ms": payload.ttl_ms,
        "mode": payload.mode,
    }
    if not consensus.is_leader:
        return await cluster.post_json(
            f"{cluster.leader_url()}/internal/lock/command",
            command,
            encrypt=True,
        )
    ok, result = await consensus.append_entry(command)
    if not ok:
        raise HTTPException(status_code=409, detail=result)
    await metrics.increment("lock_acquire_ok")
    await audit_logger.log("lock.acquire", role, {"resource": payload.resource, "owner": payload.owner})
    return result


@app.post("/lock/release")
async def lock_release(
    payload: LockReleaseRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    command = {
        "id": new_id(),
        "op": "lock.release",
        "resource": payload.resource,
        "owner": payload.owner,
    }
    if not consensus.is_leader:
        return await cluster.post_json(
            f"{cluster.leader_url()}/internal/lock/command",
            command,
            encrypt=True,
        )
    ok, result = await consensus.append_entry(command)
    if not ok:
        raise HTTPException(status_code=409, detail=result)
    await metrics.increment("lock_release_ok")
    await audit_logger.log("lock.release", role, {"resource": payload.resource, "owner": payload.owner})
    return result


@app.get("/lock/status")
async def lock_status(resource: str, x_api_key: Optional[str] = Header(None)) -> Dict[str, Any]:
    require_role(x_api_key, settings.api_key_roles, "reader")
    return await lock_manager.status(resource)


@app.post("/queue/enqueue")
async def queue_enqueue(
    payload: QueueEnqueueRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    result = await queue_manager.enqueue(payload.queue, payload.payload, payload.key)
    await metrics.increment("queue_enqueue_ok")
    await audit_logger.log("queue.enqueue", role, {"queue": payload.queue})
    return result


@app.post("/queue/dequeue")
async def queue_dequeue(
    payload: QueueDequeueRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    result = await queue_manager.dequeue(payload.queue, payload.consumer)
    await metrics.increment("queue_dequeue_ok")
    await audit_logger.log("queue.dequeue", role, {"queue": payload.queue, "consumer": payload.consumer})
    return result


@app.post("/queue/ack")
async def queue_ack(
    payload: QueueAckRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    result = await queue_manager.ack(payload.queue, payload.receipt_id)
    await metrics.increment("queue_ack_ok")
    await audit_logger.log("queue.ack", role, {"queue": payload.queue, "receipt_id": payload.receipt_id})
    return result


@app.post("/cache/read")
async def cache_read(
    payload: CacheReadRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    require_role(x_api_key, settings.api_key_roles, "reader")
    entry = await cache_node.read_local(payload.key)
    if entry:
        await metrics.increment("cache_hit")
        return {"value": entry.value, "state": entry.state}

    await metrics.increment("cache_miss")

    if consensus.is_leader and cache_coordinator:
        response = await cache_coordinator.handle_read(settings.node_id, settings.node_url, payload.key)
    else:
        response = await cluster.post_json(
            f"{cluster.leader_url()}/internal/cache/read",
            {"node_id": settings.node_id, "node_url": settings.node_url, "key": payload.key},
            encrypt=True,
        )
    await cache_node.set_entry(payload.key, response.get("value"), response.get("state", "S"))
    return response


@app.post("/cache/write")
async def cache_write(
    payload: CacheWriteRequest,
    x_api_key: Optional[str] = Header(None),
) -> Dict[str, Any]:
    role = require_role(x_api_key, settings.api_key_roles, "writer")
    if consensus.is_leader and cache_coordinator:
        response = await cache_coordinator.handle_write(
            settings.node_id, settings.node_url, payload.key, payload.value
        )
    else:
        response = await cluster.post_json(
            f"{cluster.leader_url()}/internal/cache/write",
            {
                "node_id": settings.node_id,
                "node_url": settings.node_url,
                "key": payload.key,
                "value": payload.value,
            },
            encrypt=True,
        )
    await cache_node.set_entry(payload.key, payload.value, response.get("state", "M"))
    await audit_logger.log("cache.write", role, {"key": payload.key})
    return response


@app.get("/metrics")
async def get_metrics(x_api_key: Optional[str] = Header(None)) -> Dict[str, Any]:
    require_role(x_api_key, settings.api_key_roles, "reader")
    return await metrics.snapshot()


@app.get("/ml/predict")
async def ml_predict(x_api_key: Optional[str] = Header(None)) -> Dict[str, Any]:
    require_role(x_api_key, settings.api_key_roles, "reader")
    prediction = predictor.predict(steps=1)
    return {"prediction_rps": prediction, "enabled": settings.ml_enabled}


@app.post("/internal/append")
async def internal_append(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    return await consensus.handle_append(payload)


@app.post("/internal/lock/command")
async def internal_lock_command(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    ok, result = await consensus.append_entry(payload)
    if not ok:
        raise HTTPException(status_code=409, detail=result)
    return result


@app.post("/internal/queue/enqueue")
async def internal_queue_enqueue(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    return await queue_manager._enqueue_local(payload.get("queue"), payload.get("payload"))


@app.post("/internal/queue/dequeue")
async def internal_queue_dequeue(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    return await queue_manager._dequeue_local(payload.get("queue"), payload.get("consumer"))


@app.post("/internal/queue/ack")
async def internal_queue_ack(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    return await queue_manager._ack_local(payload.get("queue"), payload.get("receipt_id"))


@app.post("/internal/cache/read")
async def internal_cache_read(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    if not cache_coordinator:
        raise HTTPException(status_code=503, detail="not_leader")
    return await cache_coordinator.handle_read(
        payload.get("node_id"), payload.get("node_url"), payload.get("key")
    )


@app.post("/internal/cache/write")
async def internal_cache_write(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    if not cache_coordinator:
        raise HTTPException(status_code=503, detail="not_leader")
    return await cache_coordinator.handle_write(
        payload.get("node_id"), payload.get("node_url"), payload.get("key"), payload.get("value")
    )


@app.post("/internal/cache/invalidate")
async def internal_cache_invalidate(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    await cache_node.invalidate(payload.get("key"))
    return {"ok": True}


@app.post("/internal/cache/writeback")
async def internal_cache_writeback(request: Request) -> Dict[str, Any]:
    payload = await parse_cluster_request(request)
    value = await cache_node.writeback(payload.get("key"))
    return {"ok": True, "value": value}
