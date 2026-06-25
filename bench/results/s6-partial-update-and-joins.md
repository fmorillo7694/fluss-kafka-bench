# S6 — Partial updates + join taxonomy (live + source-verified, 2026-06-24)

## Partial updates — PROVEN LIVE (Kafka structurally can't do this)
Three independent Flink jobs each wrote only a SUBSET of columns of the same PK row:
- Writer A: `INSERT INTO profile (user_id, name)      VALUES (1,'Ada')`
- Writer B: `INSERT INTO profile (user_id, tier)      VALUES (1,'gold')`
- Writer C: `INSERT INTO profile (user_id, last_seen) VALUES (1,'2026-06-24')`

Point query result — Fluss merged them into one complete row:

    | user_id | name | tier | last_seen  |
    |       1 | Ada  | gold | 2026-06-24 |

No single writer wrote a whole row. **Rule (verified):** the written columns must
include the primary key; unwritten columns keep their prior value (start NULL).

Why Kafka can't: a Kafka record value is an opaque blob. "Update one field" = read the
whole value, modify, re-publish — no server-side per-column merge, no multi-writer
column ownership. Fluss PK tables make the row a mergeable entity. This is a genuine
capability gap, not a perf delta. Common uses: wide-table assembly from many sources,
feature stores, slowly-changing dimensions, CDC column backfill.

## Join taxonomy in Fluss (source + docs verified)
Three different things people conflate:

| Mechanism | Direction | Async? | Join-key requirement | State in Flink? |
|-----------|-----------|--------|----------------------|-----------------|
| **Lookup join** (`FOR SYSTEM_TIME AS OF`) | stream → dim | async by default (`lookup.async`) | ALL of dim PK | no (dim lives in Fluss) |
| **Prefix lookup join** | stream → dim | async by default | PREFIX of dim PK (= `bucket.key`) | no |
| **Delta join** | BOTH ways (dual-stream) | uses async lookup internally | join key = PK prefix on BOTH sides | ~none (the win) |

- **Delta join IS built on async lookups** — Flink's `DeltaJoinUtil` requires the source
  to be a `LookupTableSource` that supports **async** lookup (`isAsyncLookup`) and unwraps
  an `AsyncTableFunction`. Difference vs a lookup join: a lookup join probes ONE way
  (stream→dim); delta join probes BOTH ways into each other's Fluss index, replacing a
  full stateful stream-stream join. No async lookup support ⇒ no delta join.
- **Both sides must be Fluss PK tables. Kafka cannot be a delta-join input** — Kafka's
  Flink source is a plain scan (not a `LookupTableSource`, no index, no async lookup), so
  it fails the first gate. To delta-join a Kafka stream: land it into a Fluss PK table
  first (Kafka → Fluss), then delta-join two Fluss tables.

## Duplicates with delta join (source-verified)
- Delta join is **eventually consistent** and "will send duplicate changes to downstream
  nodes" (comment in `DeltaJoinUtil`). The planner only allows it when
  `DuplicateChanges.ALLOW` holds — i.e. the **downstream tolerates duplicate changes**
  (idempotent **upsert sink**). If downstream requires dedup (`DISALLOW`), the optimizer
  refuses delta join and falls back to the stateful join.
- This is why the canonical shape is `insert-only source → delta join → upsert sink`:
  the upsert sink absorbs re-emitted rows idempotently by key. It's an additional silent
  gate on top of INNER-only + insert-only-sources + PK-prefix-key.

## Sync vs async lookup
`lookup.async=true` by default (higher throughput). Force sync per-join with
`/*+ OPTIONS('lookup.async'='false') */`. Also: `lookup.insert-if-not-exists=true` makes
a lookup MISS auto-insert the key into the dimension table (regular lookups only).
