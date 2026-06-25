-- =============================================================================
-- 10 — Read & write Fluss in Flink SQL
-- Demonstrates: streaming write to a log table, upsert into a PK table,
-- streaming tail-read, and projection pushdown (column pruning).
-- =============================================================================

USE CATALOG fluss_catalog;
USE bench;

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'fluss-write-clicks';

-- A datagen source table in the default catalog produces synthetic clicks.
-- (Flink has no GENERATE_SERIES; the built-in 'datagen' connector is the way.)
CREATE TEMPORARY TABLE default_catalog.default_database.clicks_src (
  click_id BIGINT,
  user_id  BIGINT,
  url      STRING,
  ts       TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '50000',
  'number-of-rows' = '1000000',
  'fields.click_id.kind' = 'sequence',
  'fields.click_id.start' = '1',
  'fields.click_id.end' = '1000000',
  'fields.user_id.min' = '0',
  'fields.user_id.max' = '999',
  'fields.url.length' = '12'
);

-- Write: stream the generated clicks into the append-only Fluss log table.
INSERT INTO clicks_log
SELECT click_id, user_id, url, ts
FROM default_catalog.default_database.clicks_src;

-- Upsert: maintain a PK table. Re-emitting the same user_id updates in place.
SET 'pipeline.name' = 'fluss-upsert-users';
INSERT INTO users_pk
SELECT user_id, name, tier, CURRENT_TIMESTAMP
FROM (VALUES
  (CAST(1 AS BIGINT), 'Ada',  'gold'),
  (CAST(2 AS BIGINT), 'Borg', 'silver'),
  (CAST(3 AS BIGINT), 'Cleo', 'bronze')
) AS t(user_id, name, tier);

-- Read (streaming tail): start from latest and follow the changelog.
-- Run interactively in the SQL client:
--   SET 'execution.runtime-mode' = 'streaming';
--   SELECT * FROM clicks_log /*+ OPTIONS('scan.startup.mode'='latest') */;

-- Projection pushdown: Fluss only ships the two projected columns off the
-- TabletServer (columnar Arrow), not the whole row — cheaper than Kafka, which
-- must deserialize the full record. Compare TaskManager network/CPU in Grafana.
--   SELECT user_id, url FROM clicks_log /*+ OPTIONS('scan.startup.mode'='earliest') */;
