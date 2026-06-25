# Delta-join cardinality sweep (live, 2026-06-24)

The money chart. Same INNER JOIN, same data, only `table.optimizer.delta-join.strategy`
differs (AUTO=delta, NONE=stream-stream). Measured Flink checkpoint state at each
cardinality. Single TaskManager (6g, RocksDB, 10s checkpoints).

| Distinct keys | Delta join | Stream-stream join | Ratio |
|---------------|-----------|--------------------|-------|
| 1,000   | 31 KB | 97 KB    | **3×** |
| 10,000  | 39 KB | 595 KB   | **15×** |
| 50,000  | 42 KB | 2.97 MB  | **71×** |
| 200,000 | 53 KB | 13.8 MB  | **261×** |

Chart: `post/img/01_delta_join_cardinality_sweep.png`. Raw: `sweep_cardinality.csv`.

## Reading the curve
- **Delta join is essentially flat** (31→53 KB across 200× more keys). The join state
  lives in the Fluss prefix index, not Flink — so the checkpoint barely grows.
- **Stream-stream join grows linearly** with key count (both sides materialized in
  RocksDB, copied into every checkpoint).
- **The ratio compounds**: 3× → 261× over the range. Extrapolating, at millions of keys
  the stream-stream checkpoint is hundreds of MB→GB while delta join stays tens of KB.
  This is the entire reason delta join exists, and it's the strongest single argument
  for Fluss in a Flink-stateful-join shop.

## Honest scaling limits hit on a single laptop node
- The **stream-stream side destabilizes the TaskManager at high cardinality**: at ≥10k
  keys the heavier runs intermittently triggered `TimeoutException: Heartbeat of
  TaskManager ... timed out` (GC/overload on one 6g TM). I ran each point individually
  with TM restarts between to get clean numbers. The 500k stream-stream point was not
  reliably reproducible on this hardware — itself a signal of how heavy that state is.
- **Uncapped max-throughput ingest is not measurable here**: removing the datagen rate
  cap (5M rows, parallelism 4) overwhelmed the single TM (RESTARTING, no completion)
  for BOTH Fluss and Kafka paths. So the reproducible ingest number stays the
  rate-controlled S2 result (Kafka ~98.6k vs Fluss ~88.7k rec/s at 100k rps, both
  stable). A true throughput ceiling needs a multi-node cluster — out of scope for a
  laptop repro, and we don't claim a number we couldn't measure cleanly.

## Takeaway
The checkpoint-state win is real, large, and grows with scale — the clearest "Fluss
wins" result in the whole study. The flip side (single-node instability under heavy
stream-stream state) actually *reinforces* the point: that state is exactly what delta
join removes from Flink.
