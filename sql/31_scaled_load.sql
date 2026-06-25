-- =============================================================================
-- 31 — Load the scaled dimension + fact tables from datagen.
-- Bounded inserts (they FINISH), so run this before the join jobs.
-- =============================================================================
USE CATALOG fluss_catalog;
USE scaled;

-- 200,000 distinct users.
CREATE TEMPORARY TABLE default_catalog.default_database.users_src (
  user_id BIGINT, name STRING, tier STRING, updated_at TIMESTAMP(3)
) WITH (
  'connector'='datagen',
  'number-of-rows'='200000',
  'fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='200000',
  'fields.name.length'='8','fields.tier.length'='4'
);
INSERT INTO users_pk SELECT user_id, name, tier, CURRENT_TIMESTAMP
FROM default_catalog.default_database.users_src;

-- 200,000 orders spread across those users (1 order per user keeps it simple +
-- guarantees the join matches; bump number-of-rows for a heavier fact side).
CREATE TEMPORARY TABLE default_catalog.default_database.orders_src (
  user_id BIGINT, order_id BIGINT, amount DECIMAL(10,2), status STRING, updated_at TIMESTAMP(3)
) WITH (
  'connector'='datagen',
  'number-of-rows'='200000',
  'fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='200000',
  'fields.order_id.kind'='sequence','fields.order_id.start'='1','fields.order_id.end'='200000',
  'fields.amount.min'='1','fields.amount.max'='999',
  'fields.status.length'='3'
);
INSERT INTO orders_pk SELECT user_id, order_id, amount, status, CURRENT_TIMESTAMP
FROM default_catalog.default_database.orders_src;
