# Tugas 3 - Sistem Parallel dan Terdistribusi

Project ini berisi simulator sistem terdistribusi dengan tiga komponen utama: distributed lock manager (konsensus), distributed queue, dan distributed cache coherence. Disertai integrasi ML untuk prediksi beban serta keamanan komunikasi antar-node.

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
2. API nodes:
   - http://localhost:8001/docs
   - http://localhost:8002/docs
   - http://localhost:8003/docs

## Contoh request
- Lock acquire:
  - `POST /lock/acquire` {"resource": "file-1", "owner": "client-a", "ttl_ms": 5000}
- Queue enqueue:
  - `POST /queue/enqueue` {"queue": "jobs", "payload": {"task": "work"}}
- Cache write:
  - `POST /cache/write` {"key": "k1", "value": "v1"}

Semua endpoint eksternal memakai header `X-API-Key` (default: `devkey`).

## Struktur
- `src/` kode aplikasi
- `docs/` dokumentasi dan report
- `scripts/` alat benchmarking sederhana

## Catatan
- Leader ditentukan via `LEADER_ID` dan `LEADER_URL`.
- Komunikasi antar-node memakai HMAC dan opsional enkripsi AES-GCM.
