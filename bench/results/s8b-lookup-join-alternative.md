# S8b — Lookup join: the clean alternative to a LEFT stream-stream join (live, 2026-06-24)

Direct contrast to S8. Same orders+users, but a temporal **lookup join**
(`FOR SYSTEM_TIME AS OF o.proc`) against the Fluss `users` PK table instead of a
stateful LEFT stream-stream join.

## Method
- `users` PK dimension preloaded with user_id 1,2 (user 3 deliberately MISSING).
- Lookup join started, output → `upsert-kafka` topic `lk_out` (count physical events).
- Streamed 3 orders (users 1,2,3). Then added user 3 LATE to test for retraction.

## Measured result
**Phase 1 — stream 3 orders:** `lk_out` offset = **3**. Exactly one row per order:

    {"order_id":100,"user_id":1,"amount":10,"name":"Ada"}
    {"order_id":200,"user_id":2,"amount":20,"name":"Borg"}
    {"order_id":300,"user_id":3,"amount":30,"name":null}     <- MISS (user 3 absent)

**Phase 2 — add user 3 late:** `lk_out` offset = **STILL 3**. The lookup join did NOT
retract or re-emit order 300. The lookup already happened at processing time; a later
dimension change does not reach back.

## Side-by-side with S8 (same 3 final rows)
| | S8 LEFT stream-stream join | S8b lookup join |
|---|---|---|
| Physical changelog events | **9** (3 partial + 3 retract + 3 complete) | **3** (one per order) |
| Late dimension arrival | **retracts** partial, re-emits completed | **ignored** (no retraction) |
| Flink state | both inputs materialized | none |
| Semantics | fully reprocessing (eventually correct) | point-in-time / as-of-proctime |
| Sink requirement | must be retraction/upsert-aware | append-only is fine |

## Takeaway
For left/outer **enrichment** of a stream with a Fluss dimension, the lookup join is
3× cheaper in events, stateless, and append-only-sink-friendly — at the cost of being
point-in-time (a miss stays a miss; it won't self-correct when the dimension updates).
Use the stream-stream LEFT join only when you genuinely need the retracting,
eventually-correct semantics — and pay for it in retraction traffic + state.
async by default (`lookup.async=true`); `/*+ OPTIONS('lookup.async'='false') */` forces sync.

## UPDATE — INNER vs LEFT lookup, and why earlier counts all = 1000 (2026-06-25)
Lookup join works with BOTH join types (verified live, with 1 deliberate orphan order
whose user_id has no matching dimension row):
- **INNER** `FOR SYSTEM_TIME AS OF`: output = **1000** — the orphan order is DROPPED.
- **LEFT**  `FOR SYSTEM_TIME AS OF`: output = **1001** — orphan KEPT as
  `{user_id:999999, order_id:999999, name:null, amount:42}`.

This corrects a possible misread of the S11 consistency run: there, all three strategies
gave 1000 because EVERY order had a matching user (zero misses), so LEFT ≡ INNER on that
data. LEFT only produces more rows than INNER when there are unmatched driving rows.

Join-type support summary:
- Lookup join: INNER (drops misses) OR LEFT (keeps misses as NULL); stateless, point-in-time.
- Delta join: INNER only (LEFT falls back to stateful stream-stream).
- Stream-stream join: any type, stateful; LEFT also retracts (partial -> completed).

## VERIFIED: async lookup works with INNER (plan-level, 2026-06-25)
Concern raised: is async lookup actually used for INNER, or only LEFT? Checked the plan
directly with EXPLAIN — async is NOT coupled to join type:
- INNER (default): `LookupJoin(... joinType=[InnerJoin] ... async=[ORDERED, KEY_ORDERED: false, 180000ms, 100])`
- LEFT  (default): `LookupJoin(... joinType=[LeftOuterJoin] ... async=[ORDERED, ...])`
- INNER + `/*+ OPTIONS('lookup.async'='false') */`: `joinType=[InnerJoin]` with NO async= attribute (sync).
So `async=[...]` appears for both INNER and LEFT by default and disappears only when async
is explicitly disabled. `ORDERED` = preserves input order across concurrent async calls.
