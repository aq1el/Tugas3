# Security Notes

## External API
- Gunakan `X-API-Key` untuk autentikasi eksternal.

## Internal Cluster
- `X-Cluster-Signature` memakai HMAC-SHA256.
- AES-GCM aktif jika `ENCRYPTION_KEY` diset.

## Default Dev Keys
- `API_KEY`: devkey
- `CLUSTER_HMAC_KEY`: devhmac
- `ENCRYPTION_KEY`: base64 32 bytes

Ganti value ini untuk produksi.
