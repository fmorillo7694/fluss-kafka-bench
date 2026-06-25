-- =============================================================================
-- 30 — Scaled setup for the high-cardinality delta-join vs stream-join chart.
--
-- The toy run (sql/20,21) proves the MECHANISM. This proves the PAYOFF: at high
-- key cardinality the stream-stream join's Flink state (and checkpoint size /
-- duration / recovery) climbs with the number of distinct keys, while the
-- delta-join side stays roughly flat because the state lives in Fluss.
--
-- Run:  sql-client.sh -i /opt/sql/init.sql -f /opt/sql/30_scaled_setup.sql
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS scaled;
USE scaled;

-- Dimension: ~200k users. first_row => insert-only changelog (delta-join input).
CREATE TABLE IF NOT EXISTS users_pk (
  user_id BIGINT, name STRING, tier STRING, updated_at TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='8','bucket.key'='user_id','table.merge-engine'='first_row');

-- Fact: orders keyed by (user_id, order_id) so user_id is the PK prefix + bucket key.
CREATE TABLE IF NOT EXISTS orders_pk (
  user_id BIGINT, order_id BIGINT, amount DECIMAL(10,2), status STRING, updated_at TIMESTAMP(3),
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='8','bucket.key'='user_id','table.merge-engine'='first_row');

-- Two distinct sink tables so delta-join and stream-stream runs don't collide.
CREATE TABLE IF NOT EXISTS out_delta (
  user_id BIGINT, order_id BIGINT, name STRING, tier STRING, amount DECIMAL(10,2), status STRING,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='8','bucket.key'='user_id');

CREATE TABLE IF NOT EXISTS out_stream (
  user_id BIGINT, order_id BIGINT, name STRING, tier STRING, amount DECIMAL(10,2), status STRING,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='8','bucket.key'='user_id');
