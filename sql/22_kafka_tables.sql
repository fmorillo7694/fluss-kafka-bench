-- =============================================================================
-- 22 — Kafka-backed tables + cross-system interop (Kafka <-> Fluss)
--
-- Interop is done with the STANDARD Flink Kafka connector alongside the Fluss
-- connector — there is no special bridge. (Fluss also ships a Kafka wire-protocol
-- layer, the `fluss-kafka` artifact, but that's for Kafka *clients* talking to
-- Fluss directly, not the Flink path.)
-- =============================================================================

-- Kafka tables live in the default in-memory catalog (not the Fluss catalog).
USE CATALOG default_catalog;

-- Upsert-Kafka so the join inputs behave like changelog streams (compacted).
CREATE TABLE IF NOT EXISTS users_kafka (
  user_id BIGINT,
  name    STRING,
  tier    STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE IF NOT EXISTS orders_kafka (
  order_id BIGINT,
  user_id  BIGINT,
  amount   DECIMAL(10,2),
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE IF NOT EXISTS user_order_enriched_kafka (
  user_id  BIGINT,
  order_id BIGINT,
  name     STRING,
  tier     STRING,
  amount   DECIMAL(10,2),
  status   STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'user_order_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- A plain append-only Kafka topic, mirror of the Fluss clicks_log.
CREATE TABLE IF NOT EXISTS clicks_kafka (
  click_id BIGINT,
  user_id  BIGINT,
  url      STRING,
  ts       TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'bench',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- --------------------------------------------------------------------------
-- READ Kafka -> WRITE Fluss  (ingest a Kafka topic into Fluss storage)
-- --------------------------------------------------------------------------
-- SET 'pipeline.name' = 'kafka-to-fluss';
-- INSERT INTO fluss_catalog.bench.clicks_log
-- SELECT click_id, user_id, url, ts FROM default_catalog.default_database.clicks_kafka;

-- --------------------------------------------------------------------------
-- READ Fluss -> WRITE Kafka  (publish enriched Fluss data back to Kafka)
-- --------------------------------------------------------------------------
-- SET 'pipeline.name' = 'fluss-to-kafka';
-- INSERT INTO default_catalog.default_database.user_order_enriched_kafka
-- SELECT user_id, order_id, name, tier, amount, status
-- FROM fluss_catalog.bench.user_order_enriched;
