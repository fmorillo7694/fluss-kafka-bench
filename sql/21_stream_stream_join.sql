-- =============================================================================
-- 21 — Regular stream-stream join (the baseline to compare against delta join)
--
-- Identical logical result to 20_delta_join.sql, but forced to run as a classic
-- Flink stateful stream-stream join so we can measure the checkpoint / state /
-- memory difference.
--
-- HOW WE FORCE THE BASELINE:
--   Delta join only kicks in when both inputs are Fluss PK tables with the join
--   key as prefix key. To get a normal stateful join, read the SAME data through
--   Kafka-backed tables instead (no Fluss index to look up against), so Flink has
--   to hold both sides in keyed state. See 22_kafka_tables.sql for these DDLs.
--
--   This is the apples-to-apples cost of "doing the join in Flink" vs
--   "pushing the join into Fluss".
-- =============================================================================

-- Use the default (in-memory) catalog with Kafka-backed mirrors of the data.
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'stream-stream-join-baseline';

-- (Tables users_kafka / orders_kafka are created in 22_kafka_tables.sql.)

INSERT INTO user_order_enriched_kafka
SELECT
  u.user_id,
  o.order_id,
  u.name,
  u.tier,
  o.amount,
  o.status
FROM orders_kafka AS o
INNER JOIN users_kafka AS u
  ON o.user_id = u.user_id;

-- LEFT JOIN nuance (the question you asked):
--   Swap INNER for LEFT below and it still runs as a stream-stream join here —
--   but the SAME LEFT JOIN over Fluss tables in 20_delta_join.sql will NOT use
--   delta join, because delta join is INNER-only. So an outer join always pays
--   the full Flink-state cost regardless of Fluss. That's a key "gap" finding.
--
--   FROM orders_kafka AS o LEFT JOIN users_kafka AS u ON o.user_id = u.user_id;
--
--   Outer-join state is also heavier: Flink must retain unmatched left rows and
--   may emit retractions (UPDATE_BEFORE/AFTER) when a match later arrives —
--   bigger state, more changelog traffic, longer checkpoints.
