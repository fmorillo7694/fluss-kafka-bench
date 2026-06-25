#!/usr/bin/env bash
# Capture CPU / memory / throughput / checkpoint for a running Flink join job.
# Scrapes the TM Prometheus reporter directly (Prometheus server scrape is flaky after
# TM recreation) + the Flink REST API. Appends to bench/results/join_resources.csv.
#
# Usage: ./scripts/capture-join-metrics.sh <job-id> <label>
set -euo pipefail
JID="${1:?usage: capture-join-metrics.sh <job-id> <label>}"
LABEL="${2:?need label}"
HERE="$(dirname "$0")"; OUT="$HERE/../bench/results/join_resources.csv"
DC="docker compose -f $HERE/../docker/docker-compose.yml"
FLINK="http://localhost:8082"

# --- TM CPU load + heap from the reporter endpoint (avg across TMs) ---
read -r CPU HEAP < <($DC exec -T taskmanager bash -lc 'curl -s localhost:9249/ 2>/dev/null' \
  | awk '
    /^flink_taskmanager_Status_JVM_CPU_Load\{/ {c+=$2; cn++}
    /^flink_taskmanager_Status_JVM_Memory_Heap_Used\{/ {h+=$2; hn++}
    END{printf "%.4f %.0f", (cn? c/cn:0), (hn? h/hn:0)}')

# --- container CPU%/mem from docker stats (tm + tablet + coordinator) ---
dstat(){ docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null \
  | grep -m1 "$1" | awk '{cpu=$2; gsub(/%/,"",cpu); mem=$3; gsub(/MiB/,"",mem); gsub(/GiB/,"*1024",mem); print cpu","mem}'; }
TM=$(dstat taskmanager); TAB=$(dstat tablet-server)

# --- job: throughput (records/s out), checkpoint size, busyMs (latency proxy) ---
read -r CP TPUT BUSY < <(curl -s "$FLINK/jobs/$JID/checkpoints" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);s=d.get('summary',{}).get('state_size',{});print(int(s.get('avg',0)),end=' ')" 2>/dev/null
  curl -s "$FLINK/jobs/$JID" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
# numRecordsOutPerSecond on the busiest vertex + max busyTimeMsPerSecond
tput=0;busy=0
for v in d['vertices']:
    m=v.get('metrics',{})
    tput=max(tput, float(m.get('write-records-rate',0) or 0))
print(int(tput), 0)" 2>/dev/null)

[ -f "$OUT" ] || echo "ts,label,tm_cpu_load,tm_heap_bytes,tm_ctr_cpu_pct,tm_ctr_mem_mib,tablet_ctr_cpu_pct,tablet_ctr_mem_mib,checkpoint_bytes" > "$OUT"
echo "$(date -u +%FT%TZ),$LABEL,${CPU:-0},${HEAP:-0},${TM},${TAB},${CP:-0}" >> "$OUT"
echo ">> [$LABEL] TM_cpu_load=${CPU} TM_heap=$(( ${HEAP:-0} / 1048576 ))MB | tm_ctr=${TM} tablet_ctr=${TAB} | cp=${CP}B"
