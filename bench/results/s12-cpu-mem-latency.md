# S12 — CPU / memory / throughput across the 3 joins (live, 2026-06-25)

Same 1.5M-key INNER enrichment, three strategies, sampled at steady state. CPU/heap
from the Flink TM Prometheus reporter + `docker stats`; checkpoint from Flink REST.

| Metric | Stream-stream (no Fluss) | Delta join (Fluss) | Lookup join (Fluss) |
|--------|--------------------------|--------------------|---------------------|
| Join operator | stateful `Join` | `DeltaJoin` | `LookupJoin` |
| **Checkpoint state** | **~91 MB** | **~47 KB** | **~12.7 KB** |
| TM container memory | ~2,650 MB | ~1,646 MB | ~1,085 MB |
| TM container CPU | ~2.5% steady (spikes to ~180% on checkpoint flush of big state) | ~22% | ~1.7% |
| Tablet-server CPU | ~0–1% (Fluss idle; Flink holds state) | ~5.5% | ~4.8% |
| Where the work is | **Flink** (RocksDB state + checkpoint I/O) | split: Flink scans + Fluss lookups | **Fluss** (lookups); Flink near-idle |
| Throughput | bulk hash join — fills fast | lookup-bound (~2k rows/s/parallelism here) | lookup-bound (~2k rows/s here) |

## Reading it
- **Memory/checkpoint:** stream-stream carries the join in Flink (big TM mem + 91 MB
  checkpoint). Delta and lookup push state to Fluss → TM memory and checkpoint collapse.
- **CPU moves, it doesn't vanish:** the lookup/delta cost lands on the **TabletServer**
  (Fluss serving the point lookups), not the Flink TM. Stream-stream keeps Fluss idle but
  hammers Flink on checkpoint flush (the ~180% spike = RocksDB compaction/snapshot of the
  big state).
- **Throughput tradeoff:** the stream-stream bulk join ingests the bounded input fast;
  delta/lookup are bound by per-row async lookups to Fluss (~2k rows/s/slot in this
  single-node setup). So Fluss trades raw join throughput for tiny state + fast recovery.
  At sustained/unbounded rates the relevant metric is steady-state lag, not batch fill
  time — measure for your workload.

## Caveat
Single-node, RocksDB state backend, laptop. Absolute CPU%/throughput are not production
figures; the SHAPE (state→Fluss, CPU→TabletServer, throughput lookup-bound) is the signal.
