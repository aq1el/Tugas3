# Performance Analysis Report

## Setup
- Nodes: 3 (docker compose)
- Workload: lock acquire benchmark (`scripts/bench.py`)
- Metrics: avg/min/max latency, throughput (requests/sec)

## Benchmark Steps
1. Jalankan cluster: `docker compose up --build`
2. Jalankan benchmark:
   - `python scripts/bench.py --url http://localhost:8001 --requests 200 --concurrency 20 --api-key devkey`
3. Catat output dan isi tabel berikut.

## Results (fill in)
| Scenario | Requests | Concurrency | Avg ms | Min ms | Max ms |
| --- | --- | --- | --- | --- | --- |
| Lock acquire | 200 | 5 | 81.41 | 54.32 | 178.51 |
| Lock acquire | 200 | 10 | 145.28 | 50.38 | 354.85 |
| Lock acquire | 200 | 20 | 384.00| 82.10 | 914.75 |
| Lock acquire | 200 | 50 | 783.52 | 242.38 | 1188.77 |

## Observations
- Throughput meningkat seiring concurrency sampai bottleneck quorum.
- Latency meningkat saat leader sibuk menunggu quorum.
- Kenaikan concurrency 5 -> 50 menaikkan avg latency dari 81.41 ms menjadi 783.52 ms, dan max latency naik sampai 1188.77 ms.
- Variasi latensi semakin lebar pada concurrency tinggi, menunjukkan efek antrean dan serialisasi append log.

## Comparison
- Single node vs multi node: multi node lebih robust namun ada overhead replikasi.
- Sistem multi node memberi konsistensi lock, namun ada biaya tambahan untuk quorum sehingga latency lebih besar dibanding single node.

## Optimizations
- Batched append log
- Pipeline commit
- Cache writeback batching

## Conclusion
Benchmark menunjukkan performa baik pada concurrency rendah, namun latency naik signifikan saat concurrency tinggi karena quorum dan serialisasi append log. Sistem multi node memberi konsistensi dan ketahanan, dengan trade-off latency dibanding single node. Hasil ini konsisten dengan karakteristik konsensus berbasis quorum.
