-- =============================================================================
-- CANONICAL state-comparison scenario (S10/S12). Same INNER enrichment, 3 ways.
-- Reader runs scripts/run-state-comparison.sh which loads data at the chosen
-- cardinality, then runs each strategy and records checkpoint + CPU/mem.
--
-- Tables (created once):
--   load.users   PK(user_id)            first_row  -- insert-only (delta-join input)
--   load.orders  PK(user_id, order_id)  first_row  -- user_id is the prefix/bucket key
--   load.snk_*   PK(user_id, order_id)             -- upsert sinks, one per strategy
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS load;
USE load;

CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT, name STRING, payload STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');

CREATE TABLE IF NOT EXISTS orders (
  user_id BIGINT, order_id BIGINT, amount INT, payload STRING,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');

CREATE TABLE IF NOT EXISTS snk_stream (user_id BIGINT, order_id BIGINT, name STRING, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id');
CREATE TABLE IF NOT EXISTS snk_delta  (user_id BIGINT, order_id BIGINT, name STRING, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id');
CREATE TABLE IF NOT EXISTS snk_lookup (user_id BIGINT, order_id BIGINT, name STRING, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id');

-- ---- strategy 1: stream-stream INNER join (no Fluss optimization) -----------
-- run with:  SET 'table.optimizer.delta-join.strategy'='NONE';
--   INSERT INTO snk_stream SELECT o.user_id,o.order_id,u.name,o.amount
--   FROM orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o
--   INNER JOIN users /*+ OPTIONS('scan.startup.mode'='earliest') */ u ON o.user_id=u.user_id;

-- ---- strategy 2: delta join (Fluss holds the join state) --------------------
-- identical query, but SET 'table.optimizer.delta-join.strategy'='AUTO';
--   ... INSERT INTO snk_delta ...

-- ---- strategy 3: lookup join (proctime enrichment against Fluss dim) --------
--   CREATE TEMPORARY VIEW op AS SELECT user_id,order_id,amount,PROCTIME() AS proc
--     FROM orders /*+ OPTIONS('scan.startup.mode'='earliest') */;
--   INSERT INTO snk_lookup SELECT o.user_id,o.order_id,u.name,o.amount
--   FROM op o LEFT JOIN users
--     /*+ OPTIONS('lookup.cache'='PARTIAL','lookup.partial-cache.max-rows'='300000') */
--     FOR SYSTEM_TIME AS OF o.proc AS u ON o.user_id=u.user_id;
