-- =============================================================================
-- S10 — Drive Flink stream-stream join state to >=100 MB, then measure the same
-- workload with delta join and lookup join. Fat payload so state-per-key is large.
-- ~300k keys x (~400B payload x2 sides) -> ~100+ MB stateful-join checkpoint.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS load;
USE load;

-- users dimension: PK user_id, fat payload. first_row => insert-only (delta-join input).
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT, name STRING, payload STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');

-- orders fact: PK (user_id, order_id) so user_id is the prefix index; fat payload.
CREATE TABLE IF NOT EXISTS orders (
  user_id BIGINT, order_id BIGINT, amount INT, payload STRING,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');

-- sinks (upsert) for delta/stream INNER joins
CREATE TABLE IF NOT EXISTS snk (
  user_id BIGINT, order_id BIGINT, name STRING, amount INT,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='4','bucket.key'='user_id');
