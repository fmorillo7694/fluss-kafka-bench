-- =============================================================================
-- S2 — Ingest throughput + latency: FLUSS side.
-- Datagen -> Fluss log table at a fixed rate. Run alongside 40_ingest_kafka.sql
-- (separately, to isolate resource cost) and compare records/s + TM CPU + bytes.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS bench;
USE bench;

CREATE TABLE IF NOT EXISTS ingest_fluss (
  id BIGINT, payload STRING, event_ts TIMESTAMP(3)
) WITH ('bucket.num'='4','table.log.format'='ARROW');

SET 'pipeline.name' = 'S2-ingest-fluss';

CREATE TEMPORARY TABLE default_catalog.default_database.gen_f (
  id BIGINT, payload STRING, event_ts AS CURRENT_TIMESTAMP
) WITH (
  'connector'='datagen',
  'rows-per-second'='100000',
  'number-of-rows'='2000000',
  'fields.id.kind'='random','fields.id.min'='1','fields.id.max'='1000000000',
  'fields.payload.length'='64'
);

INSERT INTO ingest_fluss SELECT id, payload, event_ts
FROM default_catalog.default_database.gen_f;
