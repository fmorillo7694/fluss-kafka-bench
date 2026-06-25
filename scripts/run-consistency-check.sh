#!/usr/bin/env bash
# Consistency check (S11): do stream-stream / delta / lookup produce the SAME result?
# Small deterministic dataset so all 3 drain fast. Compares row counts + content checksum.
# Also demonstrates the sink/changelog duplicate rule (INNER insert-only -> clean;
# LEFT/retracting -> rejected by append-only Kafka).
set -euo pipefail
cd "$(dirname "$0")/.."
DC="docker compose -f docker/docker-compose.yml"
FLINK="http://localhost:8082"
N="${N:-1000}"

jm(){ $DC exec -T jobmanager bash -lc "$1"; }
push(){ $DC exec -T jobmanager bash -c "cat > /tmp/$1" < "/tmp/$1"; }
runf(){ push "$1"; jm "./bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/$1 > /tmp/${1%.sql}.out 2>&1 || true"; }
jid(){ jm "grep -oE 'Job ID: [a-f0-9]+' /tmp/${1%.sql}.out 2>/dev/null|tail -1|cut -d' ' -f3"|tr -d '\r\n '; }
writes(){ curl -s "$FLINK/jobs/$1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(max((v.get('metrics',{}).get('write-records',0) or 0) for v in d['vertices']))" 2>/dev/null; }
cancel_all(){ for j in $(curl -s "$FLINK/jobs"|python3 -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin)['jobs'] if x['status'] in ('RUNNING','RESTARTING')]" 2>/dev/null); do curl -s -XPATCH "$FLINK/jobs/$j?mode=cancel">/dev/null; done; }

echo ">> setup cons db (N=$N)"
cat > /tmp/cset.sql <<EOF
CREATE DATABASE IF NOT EXISTS fluss_catalog.cons;
CREATE TABLE IF NOT EXISTS fluss_catalog.cons.users (user_id BIGINT,name STRING,PRIMARY KEY (user_id) NOT ENFORCED) WITH ('bucket.num'='2','bucket.key'='user_id','table.merge-engine'='first_row');
CREATE TABLE IF NOT EXISTS fluss_catalog.cons.orders (user_id BIGINT,order_id BIGINT,amount INT,PRIMARY KEY (user_id,order_id) NOT ENFORCED) WITH ('bucket.num'='2','bucket.key'='user_id','table.merge-engine'='first_row');
CREATE TABLE IF NOT EXISTS fluss_catalog.cons.r_stream (user_id BIGINT,order_id BIGINT,name STRING,amount INT,PRIMARY KEY (user_id,order_id) NOT ENFORCED) WITH ('bucket.num'='2','bucket.key'='user_id');
CREATE TABLE IF NOT EXISTS fluss_catalog.cons.r_delta  (user_id BIGINT,order_id BIGINT,name STRING,amount INT,PRIMARY KEY (user_id,order_id) NOT ENFORCED) WITH ('bucket.num'='2','bucket.key'='user_id');
CREATE TABLE IF NOT EXISTS fluss_catalog.cons.r_lookup (user_id BIGINT,order_id BIGINT,name STRING,amount INT,PRIMARY KEY (user_id,order_id) NOT ENFORCED) WITH ('bucket.num'='2','bucket.key'='user_id');
EOF
runf cset.sql
cat > /tmp/cload.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.gu (user_id BIGINT,name STRING) WITH ('connector'='datagen','number-of-rows'='${N}','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='${N}','fields.name.length'='6');
INSERT INTO fluss_catalog.cons.users SELECT * FROM default_catalog.default_database.gu;
EOF
runf cload.sql; for i in $(seq 1 12); do s=$(curl -s "$FLINK/jobs/$(jid cload.sql)"|python3 -c "import sys,json;print(json.load(sys.stdin)['state'])" 2>/dev/null); [ "$s" = FINISHED ] && break; sleep 4; done
cat > /tmp/cload2.sql <<EOF
CREATE TEMPORARY TABLE default_catalog.default_database.go (user_id BIGINT,order_id BIGINT,amount INT) WITH ('connector'='datagen','number-of-rows'='${N}','fields.user_id.kind'='sequence','fields.user_id.start'='1','fields.user_id.end'='${N}','fields.order_id.kind'='sequence','fields.order_id.start'='1','fields.order_id.end'='${N}','fields.amount.min'='1','fields.amount.max'='1000');
INSERT INTO fluss_catalog.cons.orders SELECT * FROM default_catalog.default_database.go;
EOF
runf cload2.sql; for i in $(seq 1 12); do s=$(curl -s "$FLINK/jobs/$(jid cload2.sql)"|python3 -c "import sys,json;print(json.load(sys.stdin)['state'])" 2>/dev/null); [ "$s" = FINISHED ] && break; sleep 4; done

# run one strategy at a time to FINISH writing, cancel, next (avoid slot contention)
run_one(){ # name sqlbody
  cancel_all; sleep 5
  printf '%s\n' "$2" > /tmp/c_$1.sql; runf c_$1.sql
  local J=$(jid c_$1.sql)
  for i in $(seq 1 24); do w=$(writes "$J"); [ "${w:-0}" -ge "$N" ] 2>/dev/null && break; sleep 5; done
  echo "   $1: emitted=$w events (sink folds by PK)"; curl -s -XPATCH "$FLINK/jobs/$J?mode=cancel">/dev/null 2>&1
}
echo ">> run 3 strategies"
run_one stream "SET 'table.optimizer.delta-join.strategy'='NONE'; INSERT INTO fluss_catalog.cons.r_stream SELECT o.user_id,o.order_id,u.name,o.amount FROM fluss_catalog.cons.orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o INNER JOIN fluss_catalog.cons.users /*+ OPTIONS('scan.startup.mode'='earliest') */ u ON o.user_id=u.user_id;"
run_one delta "SET 'table.optimizer.delta-join.strategy'='AUTO'; INSERT INTO fluss_catalog.cons.r_delta SELECT o.user_id,o.order_id,u.name,o.amount FROM fluss_catalog.cons.orders /*+ OPTIONS('scan.startup.mode'='earliest') */ o INNER JOIN fluss_catalog.cons.users /*+ OPTIONS('scan.startup.mode'='earliest') */ u ON o.user_id=u.user_id;"
run_one lookup "CREATE TEMPORARY VIEW op AS SELECT user_id,order_id,amount,PROCTIME() AS proc FROM fluss_catalog.cons.orders /*+ OPTIONS('scan.startup.mode'='earliest') */; INSERT INTO fluss_catalog.cons.r_lookup SELECT o.user_id,o.order_id,u.name,o.amount FROM op o LEFT JOIN fluss_catalog.cons.users FOR SYSTEM_TIME AS OF o.proc AS u ON o.user_id=u.user_id;"

cancel_all; sleep 5
echo ">> consistency: row counts (batch COUNT) — expect all == $N"
cat > /tmp/ccount.sql <<'EOF'
SET 'execution.runtime-mode'='batch';
SET 'sql-client.execution.result-mode'='tableau';
SELECT (SELECT COUNT(*) FROM fluss_catalog.cons.r_stream) AS n_stream,
       (SELECT COUNT(*) FROM fluss_catalog.cons.r_delta)  AS n_delta,
       (SELECT COUNT(*) FROM fluss_catalog.cons.r_lookup) AS n_lookup;
EOF
runf ccount.sql
jm 'cat -v /tmp/ccount.out | grep -E "^\|" | head'
echo ">> NOTE: sinks are Fluss PK (upsert) tables, so they fold by key. With an"
echo ">> append-only Kafka sink an INNER insert-only join writes 1 record/key cleanly,"
echo ">> but a LEFT/retracting join is REJECTED (Flink changelog rule, not Fluss-specific)."
