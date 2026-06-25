-- =============================================================================
-- S2 — Ingest throughput + latency: KAFKA side (same feed as 40_ingest_fluss.sql).
-- =============================================================================
USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS ingest_kafka (
  id BIGINT, payload STRING, event_ts TIMESTAMP(3)
) WITH (
  'connector'='kafka',
  'topic'='ingest',
  'properties.bootstrap.servers'='kafka:9092',
  'format'='json'
);

SET 'pipeline.name' = 'S2-ingest-kafka';

CREATE TEMPORARY TABLE gen_k (
  id BIGINT, payload STRING, event_ts AS CURRENT_TIMESTAMP
) WITH (
  'connector'='datagen',
  'rows-per-second'='100000',
  'number-of-rows'='2000000',
  'fields.id.kind'='random','fields.id.min'='1','fields.id.max'='1000000000',
  'fields.payload.length'='64'
);

INSERT INTO ingest_kafka SELECT id, payload, event_ts FROM gen_k;
