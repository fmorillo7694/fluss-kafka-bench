-- =============================================================================
-- S8 — LEFT JOIN over Fluss: partial results + retractions (the duplicate story)
--
-- Delta join is INNER-only, so a LEFT join runs as a REGULAR stateful stream-stream
-- join. That join is bidirectional and retracting:
--   * a left row with no match yet  -> emits partial  (left, NULL)        [+I]
--   * the matching right row arrives -> RETRACTS (left, NULL)             [-D / -U]
--                                       and emits  (left, right)          [+I / +U]
-- So one logical result can appear as 3 physical changelog events. This is exactly
-- the "recreate many results / partial duplicates" concern.
--
-- We make the timing explicit by loading the LEFT side first, then the RIGHT side.
-- =============================================================================
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS ljoin;
USE ljoin;

-- Left (driving) side: orders. Right (dimension): users.
CREATE TABLE IF NOT EXISTS orders (
  user_id BIGINT, order_id BIGINT, amount INT,
  PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH ('bucket.num'='1','table.merge-engine'='first_row');

CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT, name STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('bucket.num'='1','table.merge-engine'='first_row');

-- Append-only changelog capture: every +I/-U/+U/-D the join emits becomes a row here,
-- tagged with its RowKind, so we can SEE the partial + retraction events.
CREATE TABLE IF NOT EXISTS lj_changelog (
  op STRING, user_id BIGINT, order_id BIGINT, amount INT, name STRING
) WITH ('bucket.num'='1','table.log.format'='ARROW');

-- The LEFT JOIN job: writes the changelog of the join into lj_changelog.
-- (Run this, THEN load orders, THEN load users — see scripts/run-left-join-test.sh.)
-- INSERT INTO lj_changelog
-- SELECT '?', o.user_id, o.order_id, o.amount, u.name
-- FROM orders o LEFT JOIN users u ON o.user_id = u.user_id;
