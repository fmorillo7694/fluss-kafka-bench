#!/usr/bin/env bash
# Snapshot steady-state resource usage across the stack for one scenario run.
# Appends one row to bench/results/resources.csv. Cost axis = resource-proxy.
#
# Usage: ./scripts/snapshot-resources.sh <scenario-label> [flink-job-id]
set -euo pipefail
LABEL="${1:?usage: snapshot-resources.sh <label> [job-id]}"
JOB_ID="${2:-}"
HERE="$(dirname "$0")"
OUT="$HERE/../bench/results/resources.csv"
FLINK="${FLINK_REST:-http://localhost:8082}"
PROM="${PROM:-http://localhost:9090}"

# --- container CPU% + mem from docker stats (one-shot) ---
stat() { # service-substring  -> "cpu%,memMiB"
  local row
  row=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null \
        | grep -m1 "$1" || true)
  local cpu mem
  cpu=$(echo "$row" | awk '{print $2}' | tr -d '%'); cpu=${cpu:-0}
  mem=$(echo "$row" | awk '{print $3}' | sed 's/MiB//;s/GiB/*1024/' | bc 2>/dev/null || echo 0)
  echo "${cpu},${mem}"
}

TM=$(stat taskmanager); TABLET=$(stat tablet-server); COORD=$(stat coordinator-server)
KAFKA=$(stat "bench-kafka"); MINIO=$(stat minio)

# --- Flink checkpoint state size for the job, if given ---
CP_SIZE=0; CP_DUR=0
if [ -n "$JOB_ID" ]; then
  read -r CP_SIZE CP_DUR < <(curl -s "${FLINK}/jobs/${JOB_ID}/checkpoints" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);s=d.get('summary',{}).get('state_size',{});e=d.get('summary',{}).get('end_to_end_duration',{});print(s.get('avg',0),e.get('avg',0))" 2>/dev/null || echo "0 0")
fi

# --- MinIO bucket bytes (tiered + remote storage footprint) ---
MINIO_BYTES=$(docker compose -f "$HERE/../docker/docker-compose.yml" exec -T minio \
  sh -c 'du -sb /data 2>/dev/null | cut -f1' 2>/dev/null || echo 0)

if [ ! -f "$OUT" ]; then
  echo "ts,label,tm_cpu,tm_mem_mib,tablet_cpu,tablet_mem,coord_cpu,coord_mem,kafka_cpu,kafka_mem,minio_cpu,minio_mem,cp_state_avg_bytes,cp_dur_avg_ms,minio_bytes" > "$OUT"
fi
echo "$(date -u +%FT%TZ),${LABEL},${TM},${TABLET},${COORD},${KAFKA},${MINIO},${CP_SIZE},${CP_DUR},${MINIO_BYTES}" >> "$OUT"
echo ">> snapshot[$LABEL]:"; tail -1 "$OUT"
