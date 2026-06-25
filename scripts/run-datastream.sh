#!/usr/bin/env bash
# Submit a DataStream benchmark job via the Flink REST API.
#
# Why not `flink run`? On the flink:2.2.1 image the CLI client JVM inherits
# env.java.opts.all from config.yaml and mis-parses the long --add-opens string
# ("Unable to parse --add-opens"). Submitting the uploaded jar through the REST
# API sidesteps the client JVM entirely and works reliably.
#
# Usage:
#   ./scripts/run-datastream.sh FlussWriteJob       "--records 1000000 --rps 50000"
#   ./scripts/run-datastream.sh FlussReadToKafkaJob ""
#   ./scripts/run-datastream.sh KafkaReadToFlussJob ""
set -euo pipefail
CLASS="${1:?usage: run-datastream.sh <SimpleClassName> \"<args>\"}"
ARGS="${2:-}"
FLINK="${FLINK_REST:-http://localhost:8082}"
JAR=../flink-jobs/target/fluss-kafka-bench-1.0.0.jar
cd "$(dirname "$0")"

[ -f "$JAR" ] || { echo "jar not found — run ./scripts/build-jobs.sh first"; exit 1; }

echo ">> uploading jar ..."
JAR_ID=$(curl -s -F "jarfile=@${JAR}" "${FLINK}/jars/upload" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['filename'].split('/')[-1])")

echo ">> running io.medium.flussbench.datastream.${CLASS} ..."
curl -s -XPOST "${FLINK}/jars/${JAR_ID}/run" -H 'Content-Type: application/json' \
  -d "{\"entryClass\":\"io.medium.flussbench.datastream.${CLASS}\",\"programArgs\":\"${ARGS}\"}"
echo ""
echo ">> see the job at ${FLINK} (Flink Web UI)."
