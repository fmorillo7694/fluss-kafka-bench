-- =============================================================================
-- S8c — THE hybrid pattern: Kafka driving stream + lookup join into a Fluss
--       dimension. "Kafka as the bus, Fluss as the state/serving tier."
--
-- A Kafka topic of orders is enriched, per event, against a Fluss PK `users`
-- table via FOR SYSTEM_TIME AS OF — no external KV store, no holding the
-- dimension in Flink state. This is the realistic complement architecture.
-- =============================================================================

-- Fluss dimension (PK table, indexed, lookupable).
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS hybrid;
USE hybrid;
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT, name STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='1','table.merge-engine'='first_row');

-- Kafka driving stream (orders). proctime attr is required for the temporal lookup.
USE CATALOG default_catalog;
CREATE TABLE orders_kafka (
  order_id BIGINT, user_id BIGINT, amount INT,
  proc AS PROCTIME()
) WITH (
  'connector'='kafka',
  'topic'='orders_hybrid',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='hybrid-lookup',
  'scan.startup.mode'='earliest-offset',
  'format'='json'
);

-- Enrich the Kafka stream against the Fluss dimension (async lookup by default).
-- Output to upsert-kafka so we can count physical events (one per order = clean).
CREATE TABLE enriched_out (
  order_id BIGINT, user_id BIGINT, amount INT, name STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'='upsert-kafka','topic'='enriched_hybrid',
  'properties.bootstrap.servers'='kafka:9092',
  'key.format'='json','value.format'='json'
);

-- INSERT INTO enriched_out
-- SELECT o.order_id, o.user_id, o.amount, u.name
-- FROM orders_kafka AS o
-- LEFT JOIN fluss_catalog.hybrid.users FOR SYSTEM_TIME AS OF o.proc AS u
--   ON o.user_id = u.user_id;
