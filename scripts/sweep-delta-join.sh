#!/usr/bin/env bash
# Cardinality sweep: delta join vs stream-stream join checkpoint state at N keys.
# Produces bench/results/sweep_cardinality.csv (cardinality,mode,checkpoint_bytes,checkpoints).
#
# For each N: (re)load users_pk + orders_pk with N keys, run the join twice
# (strategy AUTO=delta, NONE=stream-stream), let it checkpoint, record avg state size.
set -euo pipefail
cd "$(dirname "$0")/.."
DC="docker compose -f docker/docker-compose.yml"
OUT="bench/results/sweep_cardinality.csv"
FLINK="http://localhost:8082"
CARDS="${CARDS:-1000 10000 50000 200000 500000}"
DB=sweep

jm() { $DC exec -T jobmanager bash -lc "$1"; }
push() { $DC exec -T jobmanager bash -c "cat > /tmp/$1" < "/tmp/$1"; }
# write the job output to a sibling .out (strip .sql) so jobid can find it
runf() { push "$1"; jm "./bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/$1 > /tmp/${1%.sql}.out 2>&1 || true"; }
jobid() { jm "grep -oE 'Job ID: [a-f0-9]+' /tmp/${1%.sql}.out 2>/dev/null | tail -1 | cut -d' ' -f3" | tr -d '\r\n '; }
wait_finish() { local j=$1; for i in $(seq 1 40); do s=$(curl -s "$FLINK/jobs/$j" | python3 -c "import sys,json;print(json.load(sys.stdin)['state'])" 2>/dev/null); { [ "$s" = FINISHED ] || [ "$s" = FAILED ]; } && break; sleep 3; done; echo "$s"; }
cancel_all() { for j in $(curl -s "$FLINK/jobs" | python3 -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin)['jobs'] if x['status'] in ('RUNNING','RESTARTING')]" 2>/dev/null); do curl -s -XPATCH "$FLINK/jobs/$j?mode=cancel">/dev/null; done; }
cp_avg() { curl -s "$FLINK/jobs/$1/checkpoints" | python3 -c "import sys,json;d=json.load(sys.stdin);s=d.get('summary',{}).get('state_size',{});c=d.get('counts',{});print(int(s.get('avg',0)),c.get('completed',0))" 2>/dev/null; }

[ -f "$OUT" ] || echo "cardinality,mode,checkpoint_bytes,checkpoints" > "$OUT"

# one-time DB + tables (single-field-key friendly; user_id is PK on users, prefix on orders)
printf "CREATE DATABASE IF NOT EXISTS fluss_catalog.%s;\n" "$DB" > /tmp/sw_db.sql; runf sw_db.sql
printf "CREATE TABLE IF NOT EXISTS fluss_catalog.%s.users (user_id BIGINT, name STRING, PRIMARY KEY (user_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');\n" "$DB" > /tmp/sw_u.sql; runf sw_u.sql
printf "CREATE TABLE IF NOT EXISTS fluss_catalog.%s.orders (user_id BIGINT, order_id BIGINT, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id','table.merge-engine'='first_row');\n" "$DB" > /tmp/sw_o.sql; runf sw_o.sql
printf "CREATE TABLE IF NOT EXISTS fluss_catalog.%s.snk (user_id BIGINT, order_id BIGINT, name STRING, amount INT, PRIMARY KEY (user_id, order_id) NOT ENFORCED) WITH ('bucket.num'='4','bucket.key'='user_id');\n" "$DB" > /tmp/sw_s.sql; runf sw_s.sql

for N in $CARDS; do
  echo "==== cardinality $N ===="
  cancel_all; sleep 4
  # load N users + N orders (sequence keys 1..N)
  cat > /tmp/sw_lu.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.gu (user_id BIGINT, name STRING) WITH ('connector'='datagen','number-of-rows'='$N','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='$N','fields.name.length'='8');
INSERT INTO fluss_catalog.$DB.users SELECT user_id, name FROM default_catalog.default_database.gu;
EOF
  runf sw_lu.sql; wait_finish "$(jobid sw_lu)" >/dev/null
  cat > /tmp/sw_lo.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.go (user_id BIGINT, order_id BIGINT, amount INT) WITH ('connector'='datagen','number-of-rows'='$N','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='$N','fields.order_id.kind'='sequence','fields.order_id.start'='1','fields.order_id.end'='$N','fields.amount.min'='1','fields.amount.max'='999');
INSERT INTO fluss_catalog.$DB.orders SELECT user_id, order_id, amount FROM default_catalog.default_database.go;
EOF
  runf sw_lo.sql; wait_finish "$(jobid sw_lo)" >/dev/null

  for MODE in AUTO NONE; do
    cancel_all; sleep 4
    label=$([ "$MODE" = AUTO ] && echo delta || echo stream)
    cat > /tmp/sw_j.sql <<EOF
SET 'pipeline.name'='sweep-$label-$N';
SET 'table.optimizer.delta-join.strategy'='$MODE';
INSERT INTO fluss_catalog.$DB.snk
SELECT o.user_id, o.order_id, u.name, o.amount
FROM fluss_catalog.$DB.orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o
INNER JOIN fluss_catalog.$DB.users /*+ OPTIONS('scan.startup.mode'='earliest') */ u
ON o.user_id = u.user_id;
EOF
    runf sw_j.sql; J=$(jobid sw_j)
    # let it process + checkpoint a few times
    sleep 70
    read -r BYTES CPS < <(cp_avg "$J")
    echo "$N,$label,${BYTES:-0},${CPS:-0}" >> "$OUT"
    echo "  $label N=$N -> ${BYTES:-0} bytes over ${CPS:-0} checkpoints"
  done
done
cancel_all
echo ">> sweep done -> $OUT"; cat "$OUT"
