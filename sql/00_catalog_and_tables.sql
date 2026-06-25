-- =============================================================================
-- 00 — Catalog + table definitions (Flink SQL on Flink 2.2 / Fluss 0.9.1)
--
-- Run with the Flink SQL client:
--   docker compose exec jobmanager ./bin/sql-client.sh -f /opt/sql/00_catalog_and_tables.sql
-- =============================================================================

-- The Fluss catalog. 'type'='fluss' is the connector identifier; bootstrap.servers
-- points at the CoordinatorServer (verified option keys, see bench/FINDINGS.md).
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS bench;
USE bench;

-- ---------------------------------------------------------------------------
-- Log table (append-only). Analogous to a Kafka topic, but columnar (Arrow).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clicks_log (
  click_id    BIGINT,
  user_id     BIGINT,
  url         STRING,
  ts          TIMESTAMP(3)
) WITH (
  'bucket.num' = '4',
  'table.log.format' = 'ARROW'
);

-- ---------------------------------------------------------------------------
-- PrimaryKey table (updatable, upsert). This is what makes delta join and
-- high-QPS lookup join possible — the PK is an index inside Fluss.
-- bucket.key = join/lookup key so it can serve prefix-key lookups.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users_pk (
  user_id     BIGINT,
  name        STRING,
  tier        STRING,
  updated_at  TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4',
  'bucket.key' = 'user_id',
  -- first_row => insert-only changelog ([I]), required for delta join inputs
  'table.merge-engine' = 'first_row'
);

-- A second PK table for the delta-join comparison (orders keyed by user_id so
-- the join key is the bucket/prefix key on BOTH sides — a delta-join requirement).
-- NOTE: the join key (user_id) must be the PREFIX of the primary key AND the
-- bucket key for delta join to engage — so PK is (user_id, order_id), not
-- (order_id, user_id). Getting this order wrong makes the planner silently fall
-- back to a regular stateful join (verified: the FORCE strategy throws
-- "doesn't support to do delta join optimization" otherwise).
CREATE TABLE IF NOT EXISTS orders_pk (
  user_id     BIGINT,
  order_id    BIGINT,
  amount      DECIMAL(10,2),
  status      STRING,
  updated_at  TIMESTAMP(3),
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4',
  'bucket.key' = 'user_id',
  -- first_row => insert-only changelog ([I]), required for delta join inputs.
  -- (For a CDC source instead, drop this and set table.delete.behavior=IGNORE.)
  'table.merge-engine' = 'first_row'
);

-- A wide PK target to demonstrate projection pushdown / column pruning.
CREATE TABLE IF NOT EXISTS user_order_enriched (
  user_id     BIGINT,
  order_id    BIGINT,
  name        STRING,
  tier        STRING,
  amount      DECIMAL(10,2),
  status      STRING,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4',
  'bucket.key' = 'user_id'
);
