# Security Notes

## External API
- Gunakan `X-API-Key` untuk autentikasi eksternal.
- RBAC berdasarkan `API_KEYS`, contoh: `admin:devkey,reader:readkey`.

## Internal Cluster
- `X-Cluster-Signature` memakai HMAC-SHA256.
- AES-GCM aktif jika `ENCRYPTION_KEY` diset.

## Audit Logging
- Log append-only disimpan di `AUDIT_LOG_PATH`.
- Setiap record di-hash dengan HMAC untuk tamper-evident logs.

## Default Dev Keys
- `API_KEY`: devkey
- `CLUSTER_HMAC_KEY`: devhmac
- `ENCRYPTION_KEY`: base64 32 bytes

Ganti value ini untuk produksi.
