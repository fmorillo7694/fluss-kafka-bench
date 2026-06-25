#!/usr/bin/env bash
# Bring up the full benchmark stack and wait for health.
set -euo pipefail
HERE="$(dirname "$0")"

# Connector jars must be on the Flink SQL classpath before the cluster starts.
"$HERE/fetch-connectors.sh"

cd "$HERE/../docker"

echo ">> Starting Fluss + Kafka + Flink 2.2 + Prometheus/Grafana ..."
docker compose up -d

echo ">> Waiting for Kafka and Fluss coordinator to be reachable ..."
docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9092 >/dev/null 2>&1 || true

cat <<EOF

Stack is starting. Endpoints:
  Flink Web UI    http://localhost:8082
  Grafana         http://localhost:3000   (anonymous admin)
  Prometheus      http://localhost:9090
  MinIO console   http://localhost:9101   (minioadmin / minioadmin)

Next:
  ./scripts/build-jobs.sh          # mvn package the Flink jobs
  ./scripts/init-tables.sh         # create Fluss catalog + tables
  ./scripts/run-delta-join.sh      # headline comparison
EOF
