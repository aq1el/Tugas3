# Deployment Guide

## Prerequisites
- Docker + Docker Compose
- Python 3.8+ (untuk local run)

## Environment
- Salin `.env.example` ke `.env` dan sesuaikan nilai.
- RBAC: `API_KEYS=admin:devkey,reader:readkey`
- Redis: `REDIS_URL=redis://redis:6379/0`

## Docker Compose (recommended)
1. Jalankan cluster:
   - `docker compose up --build`
   - Alternatif: `docker compose -f docker/docker-compose.yml up --build`
2. Cek health:
   - `http://localhost:8001/health`

## Local Run
1. Install dependencies: `pip install -r requirements.txt`
2. Jalankan: `uvicorn src.main:app --host 0.0.0.0 --port 8000`

## Scaling Nodes
- Tambahkan service node baru di `docker-compose.yml` atau `docker/docker-compose.yml`.
- Pastikan `PEERS` berisi seluruh node URL agar konsensus dan hashing konsisten.
- Untuk demo scaling cepat, aktifkan `USE_PEER_REGISTRY=1` agar node mendaftarkan diri ke Redis.

## Troubleshooting
- **Port conflict**: ubah mapping port di `docker-compose.yml`.
- **Quorum failed**: pastikan semua node berjalan dan `PEERS` benar.
- **Redis unavailable**: pastikan service `redis` running.
- **401/403**: pastikan `X-API-Key` sesuai role yang dibutuhkan.
- **Simulasi partition**: set `BLOCKED_PEERS=http://node2:8000` untuk memblokir peer tertentu.
