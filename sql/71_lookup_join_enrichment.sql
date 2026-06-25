-- =============================================================================
-- S8b — LOOKUP JOIN as the clean alternative to a LEFT stream-stream join.
--
-- Contrast with S8 (70_*): a regular LEFT join emits partial (left, NULL) rows and
-- later RETRACTS them (9 changelog events for 3 final rows). A lookup join
-- (`FOR SYSTEM_TIME AS OF`) probes the Fluss dimension AT PROCESSING TIME and emits
-- exactly ONE row per left event — no dual-sided state, no retraction storm.
-- Tradeoff: point-in-time (uses the dim value as of when the row is processed), not a
-- fully-reprocessing join.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS lookupdemo;
USE lookupdemo;

-- Dimension: Fluss PK table (single-field PK — Iceberg datalake requires it).
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT, name STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='1','table.merge-engine'='first_row');

-- Driving stream: orders as a Fluss log table. A proctime attribute is required for
-- the temporal `FOR SYSTEM_TIME AS OF` lookup.
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT, user_id BIGINT, amount INT,
  proc AS PROCTIME()
) WITH ('bucket.num'='1','table.log.format'='ARROW');

-- Output (upsert-kafka so we can count physical changelog events, as in S8).
-- The lookup join statement (run via scripts/run-lookup-join-test.sh):
--   INSERT INTO lj_out
--   SELECT o.order_id, o.user_id, o.amount, u.name
--   FROM orders AS o
--   LEFT JOIN users FOR SYSTEM_TIME AS OF o.proc AS u
--     ON o.user_id = u.user_id;
-- async by default; force sync with /*+ OPTIONS('lookup.async'='false') */ on `users`.
