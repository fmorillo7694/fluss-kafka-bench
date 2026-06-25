#!/usr/bin/env bash
# S8 — Demonstrate LEFT-JOIN partial results + retractions over Fluss.
# Timeline: start the streaming join (changelog result mode) -> load LEFT rows
# (orders) -> see partial (left,NULL) -> load RIGHT rows (users) -> see the
# retraction of the partial and the re-emission of the completed row.
set -euo pipefail
cd "$(dirname "$0")/.."
JM="docker compose -f docker/docker-compose.yml exec -T jobmanager"

run() { $JM bash -lc "cat > /tmp/$1" < "/tmp/$1"; }

# 0. tables (idempotent)
cat > /tmp/lj0.sql <<'EOF'
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS ljoin; USE ljoin;
CREATE TABLE IF NOT EXISTS orders (user_id BIGINT, order_id BIGINT, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='1','table.merge-engine'='first_row');
CREATE TABLE IF NOT EXISTS users (user_id BIGINT, name STRING, PRIMARY KEY (user_id) NOT ENFORCED) WITH ('bucket.num'='1','table.merge-engine'='first_row');
EOF
$JM bash -lc 'cat > /tmp/lj0.sql' < /tmp/lj0.sql
$JM bash -lc './bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/lj0.sql >/dev/null 2>&1; echo tables-ready'

# 1. load LEFT rows first (orders for users 1,2,3 — no matching users yet)
cat > /tmp/ljL.sql <<'EOF'
INSERT INTO fluss_catalog.ljoin.orders (user_id, order_id, amount)
VALUES (CAST(1 AS BIGINT),CAST(100 AS BIGINT),10),
       (CAST(2 AS BIGINT),CAST(200 AS BIGINT),20),
       (CAST(3 AS BIGINT),CAST(300 AS BIGINT),30);
EOF
$JM bash -lc 'cat > /tmp/ljL.sql' < /tmp/ljL.sql
$JM bash -lc './bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/ljL.sql >/dev/null 2>&1; echo left-loaded'

# 2. run the LEFT JOIN in CHANGELOG result mode for ~30s capturing partials,
#    while we load the RIGHT side mid-stream to trigger retractions.
cat > /tmp/ljQ.sql <<'EOF'
SET 'execution.runtime-mode'='streaming';
SET 'sql-client.execution.result-mode'='changelog';
USE CATALOG fluss_catalog; USE ljoin;
SELECT o.user_id, o.order_id, o.amount, u.name
FROM orders o LEFT JOIN users u ON o.user_id = u.user_id;
EOF
$JM bash -lc 'cat > /tmp/ljQ.sql' < /tmp/ljQ.sql

# kick off the right-side load in the background ~12s after the query starts
( sleep 12
  cat > /tmp/ljR.sql <<'EOF'
INSERT INTO fluss_catalog.ljoin.users (user_id, name)
VALUES (CAST(1 AS BIGINT),'Ada'),(CAST(2 AS BIGINT),'Borg'),(CAST(3 AS BIGINT),'Cleo');
EOF
  $JM bash -lc 'cat > /tmp/ljR.sql' < /tmp/ljR.sql
  $JM bash -lc './bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/ljR.sql >/dev/null 2>&1'
  echo ">> [bg] right side (users) loaded" ) &

echo ">> running LEFT JOIN in changelog mode for 30s (partials first, then retractions)..."
$JM bash -lc 'timeout 30 ./bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/ljQ.sql 2>&1 | grep -E "^\| *[+-]" || true'
wait
