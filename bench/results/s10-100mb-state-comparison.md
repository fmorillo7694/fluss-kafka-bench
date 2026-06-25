# S10 — Real load: ~100 MB Flink state, then delta join & lookup join (live, 2026-06-25)

The definitive apples-to-apples. Same data (1.5M distinct keys, ~400-byte payload per
row, both sides), same INNER enrichment, three execution strategies. Measures: if a
plain Flink stream-stream join carries ~100 MB of state, what does Fluss bring it to?

## Setup
- `load.users`: 1,500,000 rows, PK user_id, 400B payload, first_row (insert-only).
- `load.orders`: 1,500,000 rows, PK (user_id, order_id), 400B payload, first_row.
- Single TaskManager, 10g process (5.2 GB direct memory), RocksDB, 10s checkpoints.

## Result — Flink checkpoint state, same workload

| Strategy | Operator | Checkpoint state | vs stream-stream |
|----------|----------|------------------|------------------|
| **Stream-stream join** (no Fluss) | stateful `Join` (2 sources) | **~87 MB** (peak 87.8) | baseline |
| **Delta join** (Fluss, AUTO) | `DeltaJoin` | **~48 KB** (peak 56) | **~1,800× smaller** |
| **Lookup join** (Fluss, proctime) | `LookupJoin` (chained, no dim source) | **~12.5 KB** | **~7,000× smaller** |

(Target was 100 MB; the INNER join prunes the payload for matched unique keys so 1.5M
keys settled at ~87 MB of retained state — close enough; it's the same order of
magnitude and the ratios are the point. A non-pruning workload, e.g. outer/aggregating,
would hold the full payload and blow well past 100 MB.)

## What each looks like operationally
- **Stream-stream**: both 1.5M-row sides materialized in Flink keyed state (RocksDB),
  written into every checkpoint and reloaded on recovery. ~87 MB here; with the full
  payload retained (outer joins, see below) it's hundreds of MB → GB.
- **Delta join**: `DeltaJoin` operator holds ~nothing; each input row does a prefix-key
  lookup into the OTHER table's Fluss index. State lives in Fluss.
- **Lookup join**: a single chained `Source → LookupJoin → Sink`; the dimension is never
  a Flink source at all — it's looked up async in Fluss. Smallest state of the three.

## Join semantics × duplicates (cross-referencing S7/S8/S8c/S9)
- **INNER + delta join**: emits duplicate *changes* (eventually consistent) → requires a
  duplicate-tolerant upsert sink (Fluss PK / upsert-kafka). Append-only Kafka sink is
  rejected/wrong (DuplicateChanges.ALLOW gate, S7).
- **LEFT join**: NOT a delta join (INNER-only) → falls back to stateful stream-stream,
  emits partial `(left, NULL)` then retracts + re-emits when the match arrives (9 events
  for 3 rows, S8). Carries the full ~87 MB-class state too.
- **Lookup join (LEFT)**: one event per input row, a miss is a clean `NULL`, NO retraction
  when the dimension later updates (point-in-time). Cheapest, but point-in-time semantics
  (S8b/S8c).
- **Kafka as the dimension**: proctime lookup is REJECTED ("Processing-time temporal join
  is not supported yet"); only an event-time versioned join works, and it materializes the
  dimension into Flink state (78 KB for 2 rows; scales with dimension size). S9.

## Bottom line for the post
On a workload where plain Flink holds ~87 MB of join state, Fluss delta join brings it to
~48 KB and lookup join to ~12.5 KB — 1,800–7,000× less checkpointed state, which is the
difference between slow/heavy checkpoints+recovery and near-instant ones. The catch:
delta join is INNER-only and needs an upsert sink; lookup join is point-in-time; LEFT
stream-stream joins get none of this and also pay the retraction cost.

## Stability caveat
Even at 10g TM with raised host memory, the 1.5M-key lookup fan-out and heavy
stream-stream state intermittently crashed the single TaskManager / dropped the Fluss
tablet registration; numbers above are from clean checkpoint windows captured before any
restart. A multi-node cluster would be needed for sustained runs at this scale.
