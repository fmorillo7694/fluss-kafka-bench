# S11 — Consistency across the 3 joins + the sink/duplicate truth (live, 2026-06-25)

Answers two things precisely: (a) do stream-stream / delta / lookup produce the SAME
result, and (b) the duplicate question — which conflates "events emitted" with "records
stored", and depends entirely on the SINK + the join's changelog mode.

## Consistency (1000-key deterministic dataset, all 3 into Fluss PK sinks)
| Result table | Row count |
|--------------|-----------|
| r_stream (stream-stream INNER) | 1000 |
| r_delta (delta join) | 1000 |
| r_lookup (lookup join) | 1000 |

All three converge to the SAME 1000 rows. (Content checksum: stream = lookup =
{n:1000, sum_amt:500947, sum_namelen:6000}; delta converges to the same after its
changelog settles.) Caveat: comparing changelog tables with FULL OUTER JOIN / EXCEPT
shows transient non-zero diffs MID-CONVERGENCE that retract to 0 — a streaming artifact,
not a real inconsistency. Use converged aggregates, not point-in-time diff counts.

## The "2000" — corrected
Earlier I saw a delta-join job report `write-records = 2000` and implied delta join
doubles output. That was WRONG / misattributed:
- **Verified via `EXPLAIN CHANGELOG_MODE`:** delta join over insert-only (`first_row`)
  sources produces **`changelogMode=[I]`** — pure inserts, no retractions.
- Delta join → **append-only Kafka** landed **exactly 1000 physical records** for 1000
  keys. No duplicates.
- The earlier "2000" came from the `load.*` tables being re-loaded/reprocessed during the
  scaling runs (300k then appended to 1.5M, plus a job restart), not from delta join.

## The real duplicate truth (your point — and it IS true for the right cases)
Whether duplicates/retractions hit a sink depends on the join's CHANGELOG MODE and the
SINK type:

| Join | Output changelog | Append-only `kafka` sink | Upsert sink (Fluss PK / `upsert-kafka`) |
|------|------------------|--------------------------|------------------------------------------|
| INNER, insert-only sources (delta or stream) | `[I]` | OK — 1 record/key | OK — 1 row/key |
| INNER over CDC/updating sources | `[I,UA,D]` | REJECTED | folds by key |
| LEFT / outer join | `[I,UA,D]` (retractions) | **REJECTED** | folds by key, but ~2–3× changelog EVENTS |

Proven live:
- delta(INNER, first_row) → append kafka: topic offset = **1000** (clean).
- LEFT join → append kafka: rejected at planning —
  `TableException: Table sink 'left_append' doesn't support consuming update and delete
   changes which is produced by node Join(LeftOuterJoin...)`.

So: **if your sink is an append-only Kafka topic, a retracting (LEFT/updating) join
can't even write to it** — you're forced onto an upsert sink, where the retractions
become extra changelog *events* (S8: 9 events for 3 final rows), i.e. ~2–3× downstream
traffic (not stored duplicates, but real load). An INNER insert-only join is the only
shape that writes cleanly to append-only Kafka.

## IMPORTANT: this is Flink semantics, NOT a Fluss property
The retraction behavior and the append-sink rejection are **pure Flink changelog
semantics** — identical whether the join sources/sinks are Fluss, Kafka, JDBC, files,
etc. The LEFT join that got rejected ran as a regular stateful `Join(LeftOuterJoin)`
(LEFT is not delta-join-eligible). A plain Flink `orders LEFT JOIN users` with BOTH
sides as Kafka topics and no Fluss anywhere emits the same `[I,UA,D]` changelog and is
rejected by an append-only Kafka sink in exactly the same way.

So "would I get duplicate writes / retractions?" — YES, and it's the SAME with or
without Fluss, because it's Flink doing the join. What Fluss changes is orthogonal:
(1) WHERE the join state lives (Fluss index vs Flink RocksDB → the 87 MB → 48 KB
result), (2) that an INNER delta join exists to replace the stateful join, and (3) that
a non-retracting lookup join is available for point-in-time enrichment. Fluss does not
change the changelog/duplicate rules — those are Flink's.

## Bottom line
- All 3 strategies are RESULT-consistent (same 1000 rows) for the INNER insert-only case.
- "Duplicates" is a sink+changelog property, not a delta-join property: insert-only INNER
  is clean everywhere; anything retracting (LEFT/CDC) requires an upsert sink and carries
  multiplied changelog events. This is exactly why a Fluss PK table (or upsert-kafka) is
  the correct sink for join output, and why append-only Kafka is only safe for insert-only
  results.
