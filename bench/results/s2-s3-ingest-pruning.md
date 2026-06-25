# S2 ingest + S3 pruning (live, 2026-06-24)

## S2 — Ingest throughput: Fluss vs Kafka (2,000,000 rows, 64-byte payload)
Same datagen feed, same Flink cluster, written via Flink SQL INSERT.

| Side  | Duration | Throughput   | TM CPU% (snapshot) | Notes |
|-------|----------|--------------|--------------------|-------|
| Kafka | 20.3 s   | ~98,600 rec/s | 1.79               | JSON to topic |
| Fluss | 22.5 s   | ~88,700 rec/s | 0.68               | Arrow log table + remote tier |

- **Kafka ~11% faster on raw ingest.** Expected: Fluss's write path does Arrow
  columnar encoding and tiers to remote storage; Kafka just appends bytes.
- Fluss used *less* TM CPU in the snapshot (encoding happens server-side on the
  TabletServer, not the Flink TM) — note tablet-server CPU in resources.csv.
- Honest: single-node laptop; both are TM/datagen-bound, not a max-throughput test.
  The takeaway is "same ballpark ingest," not a precise win/loss.

## S3 — Projection / column pruning (30-col table, 1,000,000 rows)
- **Plan-level proof (solid):** `EXPLAIN` of a 2-column read shows
  `TableSourceScan(table=[[..., project=[c01]]])` — Fluss pushes the projection into
  the source, so only requested columns leave the TabletServer (columnar Arrow).
  Kafka has no equivalent; a Kafka consumer always ships the whole record and drops
  fields client-side.
- **Wall-clock (inconclusive at this scale):** reading 2 cols vs all 29 cols of 1M
  rows took ~the same time (~63–65 s) on the single local node — startup + polling
  overhead dominated, so the byte-savings didn't show up as a throughput win here.
  Fluss's published "~10x at 90% pruning" needs either byte-level instrumentation or
  much larger scale to reproduce; we could not cleanly measure it on the laptop stack.
  => Report the mechanism as proven, the magnitude as unverified-here.

## Resource snapshots
See bench/results/resources.csv (rows S2-fluss-ingest, S2-kafka-ingest) for the
per-component CPU/mem/MinIO-bytes captured during each run.
