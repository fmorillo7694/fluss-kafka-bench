#!/usr/bin/env bash
# Snapshot the metrics that matter for the delta-join vs stream-stream comparison.
# Pulls from Flink REST + Prometheus and appends a row to bench/results/<label>.csv.
#
# Usage: ./scripts/collect-metrics.sh <job-id> <label>
set -euo pipefail
JOB_ID="${1:?usage: collect-metrics.sh <job-id> <label>}"
LABEL="${2:?usage: collect-metrics.sh <job-id> <label>}"
OUT="$(dirname "$0")/../bench/results/${LABEL}.csv"

FLINK="http://localhost:8082"
PROM="http://localhost:9090"

# --- Checkpoint stats (the headline metric) ---
CP=$(curl -s "${FLINK}/jobs/${JOB_ID}/checkpoints")
LAST_SIZE=$(echo "$CP"   | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['latest']['completed']['state_size'])" 2>/dev/null || echo 0)
LAST_DUR=$(echo "$CP"    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['latest']['completed']['end_to_end_duration'])" 2>/dev/null || echo 0)

# --- TaskManager memory / CPU from Prometheus ---
q() { curl -s --data-urlencode "query=$1" "${PROM}/api/v1/query" \
        | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 0)" 2>/dev/null || echo 0; }

HEAP=$(q 'flink_taskmanager_Status_JVM_Memory_Heap_Used')
CPU=$(q 'flink_taskmanager_Status_JVM_CPU_Load')
ROCKS=$(q 'flink_taskmanager_job_task_operator_rocksdb_total_sst_files_size')

if [ ! -f "$OUT" ]; then
  echo "ts,label,checkpoint_size_bytes,checkpoint_duration_ms,tm_heap_used_bytes,tm_cpu_load,rocksdb_sst_bytes" > "$OUT"
fi
echo "$(date -u +%FT%TZ),${LABEL},${LAST_SIZE},${LAST_DUR},${HEAP},${CPU},${ROCKS}" >> "$OUT"
echo ">> appended to $OUT:"
tail -1 "$OUT"
