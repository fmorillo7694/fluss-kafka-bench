-- =============================================================================
-- S6 — Partial updates on a Fluss PK table.
-- Two independent writers update DISJOINT columns of the same primary-key row;
-- Fluss merges them into one complete row. Kafka cannot do this — a Kafka record
-- is an opaque blob; "update one field" means re-publish the whole value.
--
-- Rule (verified): the written columns must include the primary key.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS pupdate;
USE pupdate;

-- A user profile assembled from multiple sources.
CREATE TABLE IF NOT EXISTS profile (
  user_id   BIGINT,
  name      STRING,   -- owned by the "identity" stream
  tier      STRING,   -- owned by the "billing" stream
  last_seen STRING,   -- owned by the "activity" stream
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num' = '1');

-- Writer A: identity source fills only (user_id, name).
INSERT INTO profile (user_id, name)
  VALUES (CAST(1 AS BIGINT), 'Ada'), (CAST(2 AS BIGINT), 'Borg');

-- Writer B: billing source fills only (user_id, tier) for the SAME rows.
-- INSERT INTO profile (user_id, tier) VALUES (1, 'gold'), (2, 'silver');

-- Writer C: activity source fills only (user_id, last_seen).
-- INSERT INTO profile (user_id, last_seen) VALUES (1, '2026-06-24'), (2, '2026-06-23');

-- After all three, each row is complete (name + tier + last_seen) even though no
-- single writer ever wrote a whole row. Verify with a PK point query:
--   SELECT * FROM profile WHERE user_id = 1;
