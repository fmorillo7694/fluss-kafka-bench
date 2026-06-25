# Headline result: delta join vs stream-stream join (live run, 2026-06-24)

Both jobs ran the SAME query on the SAME Fluss PK tables, on the booted stack
(Flink 2.2.1 / Fluss 0.9.1, 4 task slots, checkpoint interval 10s, RocksDB backend).
The only difference: `table.optimizer.delta-join.strategy` = AUTO vs NONE.

| Metric                 | Delta join (AUTO) | Stream-stream (NONE) |
|------------------------|-------------------|----------------------|
| Join operator in graph | **DeltaJoin**     | **Join** (stateful)  |
| Checkpoint state size  | **34,622 bytes**  | 41,788 bytes         |
| Latest cp duration     | 57 ms             | 36 ms                |

## What this proves
- The optimization is real and observable: AUTO yields a `DeltaJoin` operator
  (state pushed into the Fluss prefix-key index); NONE yields a classic stateful
  `Join` that materializes both sides in Flink keyed state.
- Even here the delta-join checkpoint is smaller.

## Honest caveat (do NOT oversell)
This was a TOY dataset (6 orders, 3 users), so the absolute gap is small
(~34KB vs ~42KB) and stream-stream checkpoint duration was actually lower at this
size. The whole point of delta join is that the stream-stream side's state — and
therefore checkpoint size, checkpoint duration, and recovery time — grows with
**key cardinality**, while the delta-join side stays roughly flat (state lives in
Fluss). To show the real divergence you must run a high-cardinality workload:

  Next step for the post's money chart:
  - load users_pk with O(10^6) keys and stream a high-rate orders feed
  - watch flink_..._lastCheckpointSize and rocksdb_total_sst_files_size in Grafana
  - the stream-stream line climbs; the delta-join line stays flat

The repo is wired for this (Prometheus + Grafana + RocksDB native metrics verified
working); this file records the small-scale proof-of-mechanism, not the scaled result.
