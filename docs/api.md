# API Summary

Semua endpoint eksternal butuh header `X-API-Key`.

## Health
- `GET /health`

## Lock
- `POST /lock/acquire`
  - body: `{ "resource": "r1", "owner": "c1", "ttl_ms": 5000 }`
- `POST /lock/release`
  - body: `{ "resource": "r1", "owner": "c1" }`
- `GET /lock/status?resource=r1`

## Queue
- `POST /queue/enqueue`
  - body: `{ "queue": "jobs", "payload": {"task": "x"} }`
- `POST /queue/dequeue`
  - body: `{ "queue": "jobs", "consumer": "worker-1" }`

## Cache
- `POST /cache/read`
  - body: `{ "key": "k1" }`
- `POST /cache/write`
  - body: `{ "key": "k1", "value": "v1" }`

## Metrics & ML
- `GET /metrics`
- `GET /ml/predict`
