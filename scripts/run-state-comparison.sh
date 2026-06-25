#!/usr/bin/env bash
# Canonical reproducible run for the "where does join state live" comparison (S10/S12).
# Loads N keys with a fat payload, then runs stream-stream / delta / lookup INNER joins,
# recording checkpoint size + CPU/mem for each. Results -> bench/results/state_runs.csv
#
# Usage:  CARD=300000 PAYLOAD=400 ./scripts/run-state-comparison.sh
set -euo pipefail
cd "$(dirname "$0")/.."
DC="docker compose -f docker/docker-compose.yml"
FLINK="http://localhost:8082"
CARD="${CARD:-300000}"; PAYLOAD="${PAYLOAD:-400}"
OUT="bench/results/state_runs.csv"

jm(){ $DC exec -T jobmanager bash -lc "$1"; }
sql(){ $DC exec -T jobmanager bash -c "cat > /tmp/$1" < "/tmp/$1"; jm "./bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/$1 > /tmp/${1%.sql}.out 2>&1 || true"; }
jid(){ jm "grep -oE 'Job ID: [a-f0-9]+' /tmp/${1%.sql}.out 2>/dev/null | tail -1 | cut -d' ' -f3" | tr -d '\r\n '; }
state(){ curl -s "$FLINK/jobs/$1" | python3 -c "import sys,json;print(json.load(sys.stdin).get('state','?'))" 2>/dev/null; }
writes(){ curl -s "$FLINK/jobs/$1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(max((v.get('metrics',{}).get('write-records',0) or 0) for v in d['vertices']))" 2>/dev/null; }
cancel_all(){ for j in $(curl -s "$FLINK/jobs" | python3 -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin)['jobs'] if x['status'] in ('RUNNING','RESTARTING')]" 2>/dev/null); do curl -s -XPATCH "$FLINK/jobs/$j?mode=cancel">/dev/null; done; }
wait_finish(){ for i in $(seq 1 40); do s=$(state "$1"); { [ "$s" = FINISHED ] || [ "$s" = FAILED ]; } && break; sleep 5; done; echo "$s"; }

[ -f "$OUT" ] || echo "cardinality,payload,strategy,operator,checkpoint_bytes,tm_mem_mib,tablet_cpu_pct" > "$OUT"

echo ">> [1/4] setup tables"
$DC exec -T jobmanager bash -c 'cat > /tmp/setup.sql' < sql/90_state_comparison.sql
jm './bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/setup.sql >/dev/null 2>&1; echo ok'

echo ">> [2/4] load ${CARD} users + ${CARD} orders (payload ${PAYLOAD}B)"
cat > /tmp/lu.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.gu (user_id BIGINT,name STRING,payload STRING) WITH ('connector'='datagen','number-of-rows'='${CARD}','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='${CARD}','fields.name.length'='12','fields.payload.length'='${PAYLOAD}');
INSERT INTO fluss_catalog.load.users SELECT user_id,name,payload FROM default_catalog.default_database.gu;
EOF
sql lu.sql; wait_finish "$(jid lu.sql)" >/dev/null
cat > /tmp/lo.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.go (user_id BIGINT,order_id BIGINT,amount INT,payload STRING) WITH ('connector'='datagen','number-of-rows'='${CARD}','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='${CARD}','fields.order_id.kind'='sequence','fields.order_id.start'='1','fields.order_id.end'='${CARD}','fields.amount.min'='1','fields.amount.max'='999','fields.payload.length'='${PAYLOAD}');
INSERT INTO fluss_catalog.load.orders SELECT user_id,order_id,amount,payload FROM default_catalog.default_database.go;
EOF
sql lo.sql; wait_finish "$(jid lo.sql)" >/dev/null

run_strategy(){ # name  sqlbody  operator-substr
  local name="$1" body="$2" op="$3"
  cancel_all; sleep 5
  printf '%s\n' "$body" > /tmp/run_$name.sql
  sql run_$name.sql
  local J=$(jid run_$name.sql)
  echo "   $name job=$J — settling 80s"; sleep 80
  local cp=$(curl -s "$FLINK/jobs/$J/checkpoints" | python3 -c "import sys,json;print(int(json.load(sys.stdin).get('summary',{}).get('state_size',{}).get('avg',0)))" 2>/dev/null)
  local mem=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null | awk '/taskmanager/{m=$2; gsub(/MiB/,"",m); gsub(/GiB/,"*1024",m); print m; exit}')
  local tcpu=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' 2>/dev/null | awk '/tablet-server/{c=$2; gsub(/%/,"",c); print c; exit}')
  echo "${CARD},${PAYLOAD},${name},${op},${cp:-0},${mem:-0},${tcpu:-0}" >> "$OUT"
  echo "   -> $name checkpoint=${cp}B tm_mem=${mem}MiB tablet_cpu=${tcpu}%"
}

echo ">> [3/4] run 3 strategies"
run_strategy stream "SET 'table.optimizer.delta-join.strategy'='NONE';
INSERT INTO fluss_catalog.load.snk_stream SELECT o.user_id,o.order_id,u.name,o.amount FROM fluss_catalog.load.orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o INNER JOIN fluss_catalog.load.users /*+ OPTIONS('scan.startup.mode'='earliest') */ u ON o.user_id=u.user_id;" "Join"
run_strategy delta "SET 'table.optimizer.delta-join.strategy'='AUTO';
INSERT INTO fluss_catalog.load.snk_delta SELECT o.user_id,o.order_id,u.name,o.amount FROM fluss_catalog.load.orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o INNER JOIN fluss_catalog.load.users /*+ OPTIONS('scan.startup.mode'='earliest') */ u ON o.user_id=u.user_id;" "DeltaJoin"
run_strategy lookup "SET 'parallelism.default'='2';
CREATE TEMPORARY VIEW op AS SELECT user_id,order_id,amount,PROCTIME() AS proc FROM fluss_catalog.load.orders /*+ OPTIONS('scan.startup.mode'='earliest') */;
INSERT INTO fluss_catalog.load.snk_lookup SELECT o.user_id,o.order_id,u.name,o.amount FROM op o LEFT JOIN fluss_catalog.load.users /*+ OPTIONS('lookup.cache'='PARTIAL','lookup.partial-cache.max-rows'='300000') */ FOR SYSTEM_TIME AS OF o.proc AS u ON o.user_id=u.user_id;" "LookupJoin"

cancel_all
echo ">> [4/4] done -> $OUT"; cat "$OUT"
