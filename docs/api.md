# API Summary

Semua endpoint eksternal butuh header `X-API-Key`.
Role minimal: `reader` untuk read, `writer` untuk write (admin memiliki semua).

## Health
- `GET /health`

## Lock
- `POST /lock/acquire`
  - body: `{ "resource": "r1", "owner": "c1", "ttl_ms": 5000, "mode": "exclusive" }`
- `POST /lock/release`
  - body: `{ "resource": "r1", "owner": "c1" }`
- `GET /lock/status?resource=r1`

## Queue
- `POST /queue/enqueue`
  - body: `{ "queue": "jobs", "payload": {"task": "x"} }`
- `POST /queue/dequeue`
  - body: `{ "queue": "jobs", "consumer": "worker-1" }`
  - response: `receipt_id` dipakai untuk `/queue/ack`
- `POST /queue/ack`
  - body: `{ "queue": "jobs", "receipt_id": "..." }`

## Cache
- `POST /cache/read`
  - body: `{ "key": "k1" }`
- `POST /cache/write`
  - body: `{ "key": "k1", "value": "v1" }`

## Metrics & ML
- `GET /metrics`
- `GET /ml/predict`
