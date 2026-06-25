# S7 — Duplicate semantics: Kafka vs Fluss sinks (live, 2026-06-24)

Question: when delta join / lookups emit duplicate changes (same key written many
times), what actually happens at each sink? Measured by writing 50 rows with only
**10 distinct keys** (each key 5×) to three sinks.

| Sink | Physical records | Logical view to a consumer | Dedups? |
|------|------------------|----------------------------|---------|
| Append-only Kafka (`connector=kafka`) | **50** (offset=50) | **50** — every dup is a permanent event | No |
| Upsert Kafka (`connector=upsert-kafka`) | **50** (offset=50) | **10** — read as changelog, folds by key | Logically only |
| Fluss PK table | **10** | **10** — point-queryable current value | Yes, at write time |

Method: `kafka-get-offsets.sh` for physical counts; batch `COUNT(*)` for Fluss;
streaming changelog read for upsert-kafka (the output showed `+U/-U` retraction pairs
converging to 10 — that's the fold-by-key happening live).

## What this means for delta join + lookups
- **Append-only Kafka does NOT dedup.** Delta join is eventually-consistent and re-emits
  duplicate changes; into a plain `kafka` topic every duplicate becomes a permanent
  physical record a consumer must see. This is exactly why Flink's planner REFUSES delta
  join when the sink can't collapse duplicates (`DuplicateChanges.DISALLOW`) — verified
  earlier in `DeltaJoinUtil.canJoinOutputDuplicateChanges` and now shown end-to-end.
- **Upsert Kafka absorbs duplicates logically** (10 rows materialized) but pays for it:
  the topic still physically holds all 50 until async/best-effort compaction, and the
  changelog carries `-U/+U` retraction PAIRS (≈2× the downstream message traffic).
- **Fluss PK table dedups at write time** — 10 physical rows, instantly point-queryable,
  no retraction pairs, no compaction lag. This is *why* a Fluss PK table is the natural
  delta-join/lookup SINK and SOURCE: a duplicate is just an upsert onto the same key.

## Transport vs logical duplicates (don't conflate)
- **Transport dups** (Flink retry/failover redelivery): handled by an EXACTLY_ONCE
  transactional Kafka sink or Fluss's idempotent writer (`client.writer.enable-idempotence`
  default true). Orthogonal to joins.
- **Logical dups** (delta join / eventual consistency re-emitting the same change):
  handled ONLY by a keyed sink that collapses by key (Fluss PK or upsert-kafka), never by
  an append-only topic. This scenario measured the logical kind.

## Lookup-join angle
A lookup join against a Fluss PK table always sees the single current value per key (10),
no matter how many times written. A lookup against a compacted Kafka topic requires the
consumer to materialize the changelog first (the +U/-U folding above) — extra work Fluss
does server-side.
