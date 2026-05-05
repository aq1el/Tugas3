# Architecture

## Overview
- Setiap node menjalankan FastAPI service yang berperan sebagai:
  - Distributed lock manager (konsensus append log)
   - Distributed queue (consistent hashing + Redis persistence)
   - Distributed cache coherence (MESI-like directory + LRU/LFU replacement)
- Leader dipilih statis via `LEADER_ID` dan bertindak sebagai koordinator konsensus dan cache.
- Komunikasi antar-node memakai HMAC signature dan opsional AES-GCM.

## Diagram (Mermaid)
```mermaid
flowchart LR
  Client -->|API| NodeA
  Client -->|API| NodeB
  Client -->|API| NodeC

  subgraph Cluster
    NodeA <--> NodeB
    NodeB <--> NodeC
    NodeA <--> NodeC
  end

  NodeA -->|Raft-like append| NodeB
  NodeA -->|Raft-like append| NodeC

  NodeA -->|Cache directory| NodeB
  NodeA -->|Cache directory| NodeC
```

## Core Components
1. **Consensus (Raft-like)**
   - Leader menerima command dan mereplikasi log ke follower.
   - Commit jika quorum tercapai, lalu apply ke state machine.
2. **Distributed Lock**
   - Lock disimpan sebagai resource + owner + TTL.
   - Mendukung shared/exclusive locks dan deteksi deadlock (wait-for graph).
   - Keputusan lock dilakukan melalui consensus log.
3. **Distributed Queue**
   - Sharding via consistent hashing.
   - Producer/consumer bisa hit node mana pun, request akan diteruskan ke node yang tepat.
   - At-least-once delivery memakai receipt/ack dan reclaim timeout.
4. **Cache Coherence (MESI-like)**
   - Directory di leader mengatur state M/E/S/I.
   - Invalidate dan writeback dikirim ke node lain sesuai state.

## Security
- `X-API-Key` untuk akses eksternal.
- RBAC sederhana berdasarkan mapping API key -> role.
- `X-Cluster-Signature` untuk autentikasi antar-node.
- AES-GCM (opsional) jika `ENCRYPTION_KEY` diisi.
- Audit log menggunakan HMAC chain untuk tamper-evident logging.

## Failure Handling
- Queue disimpan di Redis sehingga node failure tidak menghilangkan data.
- Network partition dapat disimulasikan via `BLOCKED_PEERS`, operasi akan gagal jika quorum tidak terpenuhi.
- Peer registry berbasis Redis (`USE_PEER_REGISTRY`) memungkinkan node discovery saat scaling.
