# S8 — LEFT JOIN over Fluss: partial results + retractions (live, 2026-06-24)

Confirms the intuition: a LEFT join from Fluss does NOT use delta join (INNER-only),
runs as a **regular stateful stream-stream join**, and produces partial results that get
retracted and re-emitted — "many results / duplicates with partial results."

## Method
- `orders` (left, PK order_id, user_id as a column) and `users` (right, PK user_id),
  both Fluss `first_row` tables. Output → an `upsert-kafka` topic so every changelog
  event (+I / -U / +U) becomes a countable physical record.
- Load 3 orders (users 1,2,3) FIRST, with NO matching users. Start the LEFT JOIN reading
  both sides from `earliest`. Then load the 3 users mid-stream.

## Measured result
**Phase 1 — orders only, no users:** `lj_out` offset = **3**. Three PARTIAL rows, each
with `name = null`:

    {"order_id":100,"user_id":1,"amount":10,"name":null}
    {"order_id":200,"user_id":2,"amount":20,"name":null}
    {"order_id":300,"user_id":3,"amount":30,"name":null}

**Phase 2 — load users 1,2,3:** `lj_out` offset = **9** (3 → 9, i.e. **+6 events**).
Each match RETRACTS the partial `(…, name=null)` and emits the completed `(…, name=Ada)`:
a `-U` then `+U` per key → 2 physical records × 3 keys = 6 more.

=> **9 physical changelog events to express 3 final logical rows.** That is the
"recreate many results / partial duplicates" effect, quantified.

## Why this happens (and the delta-join contrast)
- LEFT join can't be a delta join (INNER-only), so it's a stateful stream-stream join:
  both inputs held in Flink state; an arrival on EITHER side probes the other.
- A left row with no right match emits `(left, NULL)` immediately (partial). When the
  right arrives, the planner must correct that output → retraction `-U(left,NULL)` +
  `+U(left,right)`. This is standard Flink retracting-join semantics, not Fluss-specific —
  but it means a LEFT join's output is a **retracting changelog**, not append-only.
- Cost implications: ~2–3× downstream events vs the final row count, doubled changelog
  traffic, and a sink that MUST be retraction/upsert-aware (a plain append-only Kafka
  topic would keep the stale `(left,NULL)` partials forever — see S7). And because it's a
  full stateful join, it carries the big-checkpoint cost delta join avoids (see S1).

## Practical guidance
- If you need a LEFT/outer enrichment and want to avoid retraction storms, prefer a
  **lookup join** (`FOR SYSTEM_TIME AS OF`) against a Fluss PK dimension table: it emits
  one row per left event (the current dim value or NULL), no retraction of partials,
  no dual-sided state. The tradeoff: it's point-in-time (uses the dim value AT
  processing time), not a fully-reprocessing join.
- Only an INNER equi-join on the PK-prefix key gets the delta-join state savings.

## Side finding: Iceberg datalake forces single-field keys
With `datalake.format=iceberg` enabled cluster-wide, creating/writing a PK table with a
**composite** primary key fails:
`IllegalArgumentException: Key fields must have exactly one field for iceberg format,
but got: [user_id, order_id]`. So enabling Iceberg tiering globally constrains ALL PK
tables to single-column keys (had to redesign orders to PK(order_id) with user_id as a
plain column). Another concrete maturity/￼interop limitation.
