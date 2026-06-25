-- =============================================================================
-- S3 — Projection / column pruning. Wide table (30 cols); compare reading 2 cols
-- vs all 30. Fluss does server-side columnar (Arrow) projection pushdown; Kafka
-- ships the whole record and the client discards unused fields.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS bench;
USE bench;

-- 30-column wide log table.
CREATE TABLE IF NOT EXISTS wide_fluss (
  id BIGINT,
  c01 STRING, c02 STRING, c03 STRING, c04 STRING, c05 STRING,
  c06 STRING, c07 STRING, c08 STRING, c09 STRING, c10 STRING,
  c11 STRING, c12 STRING, c13 STRING, c14 STRING, c15 STRING,
  c16 STRING, c17 STRING, c18 STRING, c19 STRING, c20 STRING,
  c21 STRING, c22 STRING, c23 STRING, c24 STRING, c25 STRING,
  c26 STRING, c27 STRING, c28 STRING, c29 STRING
) WITH ('bucket.num'='4','table.log.format'='ARROW');

SET 'pipeline.name' = 'S3-load-wide';
CREATE TEMPORARY TABLE default_catalog.default_database.gen_w (
  id BIGINT,
  c01 STRING, c02 STRING, c03 STRING, c04 STRING, c05 STRING,
  c06 STRING, c07 STRING, c08 STRING, c09 STRING, c10 STRING,
  c11 STRING, c12 STRING, c13 STRING, c14 STRING, c15 STRING,
  c16 STRING, c17 STRING, c18 STRING, c19 STRING, c20 STRING,
  c21 STRING, c22 STRING, c23 STRING, c24 STRING, c25 STRING,
  c26 STRING, c27 STRING, c28 STRING, c29 STRING
) WITH (
  'connector'='datagen','number-of-rows'='1000000',
  'fields.id.kind'='random','fields.id.min'='1','fields.id.max'='1000000000',
  'fields.c01.length'='32','fields.c02.length'='32','fields.c03.length'='32',
  'fields.c04.length'='32','fields.c05.length'='32','fields.c06.length'='32',
  'fields.c07.length'='32','fields.c08.length'='32','fields.c09.length'='32',
  'fields.c10.length'='32','fields.c11.length'='32','fields.c12.length'='32',
  'fields.c13.length'='32','fields.c14.length'='32','fields.c15.length'='32',
  'fields.c16.length'='32','fields.c17.length'='32','fields.c18.length'='32',
  'fields.c19.length'='32','fields.c20.length'='32','fields.c21.length'='32',
  'fields.c22.length'='32','fields.c23.length'='32','fields.c24.length'='32',
  'fields.c25.length'='32','fields.c26.length'='32','fields.c27.length'='32',
  'fields.c28.length'='32','fields.c29.length'='32'
);
INSERT INTO wide_fluss SELECT * FROM default_catalog.default_database.gen_w;
