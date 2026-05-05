# Tugas 3 - Sistem Parallel dan Terdistribusi

Project ini berisi simulator sistem terdistribusi dengan tiga komponen utama: distributed lock manager (konsensus), distributed queue (persistent), dan distributed cache coherence. Disertai integrasi ML untuk prediksi beban serta keamanan komunikasi antar-node.

## Quick start (local single node)
1. Buat virtual env dan install dependencies:
   - Windows PowerShell:
     - `python -m venv .venv`
     - `.\.venv\Scripts\Activate.ps1`
     - `pip install -r requirements.txt`
2. Jalankan service:
   - `uvicorn src.main:app --host 0.0.0.0 --port 8000`
3. Buka API docs:
   - http://localhost:8000/docs

## Multi-node (docker compose)
1. Jalankan:
   - `docker compose up --build`
   - Alternatif: gunakan `docker/docker-compose.yml`
2. API nodes:
   - http://localhost:8001/docs
   - http://localhost:8002/docs
   - http://localhost:8003/docs

## Contoh request
- Lock acquire:
   - `POST /lock/acquire` {"resource": "file-1", "owner": "client-a", "ttl_ms": 5000, "mode": "exclusive"}
- Queue enqueue:
  - `POST /queue/enqueue` {"queue": "jobs", "payload": {"task": "work"}}
- Cache write:
  - `POST /cache/write` {"key": "k1", "value": "v1"}

Semua endpoint eksternal memakai header `X-API-Key`. Default role:
- admin: `devkey`
- reader: `readkey`

## Struktur
- `src/` kode aplikasi
- `docs/` dokumentasi dan report
- `scripts/` alat benchmarking sederhana
- `benchmarks/` skenario load test (locust)
- `docker/` Dockerfile dan compose alternatif

Dokumentasi tambahan:
- OpenAPI spec: `docs/api_spec.yaml`
- Deployment guide: `docs/deployment_guide.md`

## Testing
- Unit tests: `pytest -q`
- Locust: `locust -f benchmarks/load_test_scenarios.py`

## Deliverables
- Video demo: (isi link YouTube)
- Report PDF: (isi nama file PDF)

## Catatan
- Leader ditentukan via `LEADER_ID` dan `LEADER_URL`.
- Komunikasi antar-node memakai HMAC dan opsional enkripsi AES-GCM.
- Queue disimpan di Redis (`REDIS_URL`) untuk persistence dan recovery.
- Untuk scaling lebih dinamis, set `USE_PEER_REGISTRY=1` agar node mendaftar ke Redis.
