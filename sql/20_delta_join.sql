-- =============================================================================
-- 20 — DELTA JOIN (the headline)  [Flink SQL only; Flink 2.2 + Fluss 0.9.1]
--
-- Delta join turns "look up the other side from Flink state" into "look up the
-- other side from the Fluss PK-table index". The big join state lives in Fluss,
-- NOT in Flink RocksDB — so Flink checkpoints stay tiny and recovery is fast.
--
-- This is the SAME logical query as 21_stream_stream_join.sql. Run both, then
-- compare checkpoint size / duration and TaskManager memory in Grafana.
--
-- REQUIREMENTS (ALL verified by booting the stack and inspecting EXPLAIN — see
-- bench/FINDINGS.md). Miss ANY one and the planner silently falls back to a
-- regular stateful stream-stream join:
--   1. Both inputs are Fluss PrimaryKey tables.
--   2. The join key (user_id) is the bucket.key AND a prefix of the PK on BOTH
--      sides. Fluss emits a prefix-key index only when PK length > bucket-key
--      length, so orders_pk PK is (user_id, order_id) — NOT (order_id, user_id).
--   3. Inputs are INSERT-ONLY. We set 'table.merge-engine' = 'first_row' on the
--      source tables so their changelog mode is [I] (delta join only consumes
--      +I/+UA). (table.delete.behavior = IGNORE|DISABLE is the CDC alternative.)
--   4. INNER JOIN only -> LEFT/RIGHT/FULL OUTER are NOT supported.
--   5. The pattern must be  insert-only source -> join -> UPSERT SINK. Delta join
--      is chosen for the WHOLE INSERT pipeline; a bare `SELECT`/`EXPLAIN SELECT`
--      will NOT show a DeltaJoin because there's no sink to anchor it.
-- =============================================================================

USE CATALOG fluss_catalog;
USE bench;

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'delta-join-users-orders';

-- The planner applies delta join automatically (strategy AUTO) when reqs 1-5 hold
-- — there is no "DELTA JOIN" keyword. Verify it engaged with EXPLAIN on the FULL
-- INSERT (not a bare SELECT); look for a `DeltaJoin` node instead of `Join`:
--
--   EXPLAIN
--   INSERT INTO user_order_enriched SELECT ... (the statement below) ;
--
-- To make a missing requirement fail loudly instead of silently falling back:
--   SET 'table.optimizer.delta-join.strategy' = 'FORCE';   -- throws if not applicable

INSERT INTO user_order_enriched
SELECT
  o.user_id,
  o.order_id,
  u.name,
  u.tier,
  o.amount,
  o.status
FROM orders_pk AS o
INNER JOIN users_pk AS u
  ON o.user_id = u.user_id;

-- WHY CHECKPOINTS STAY SMALL:
--   A regular stream-stream join keeps BOTH sides materialized in Flink keyed
--   state (RocksDB) so a late row on either side can find its match. That state
--   grows unbounded with key cardinality and is copied into every checkpoint.
--   Delta join keeps almost no operator state: on each input row it does a
--   prefix-key lookup against the OTHER table's Fluss index. The "state" is the
--   Fluss PK table, which is durable and tiered independently of Flink.
