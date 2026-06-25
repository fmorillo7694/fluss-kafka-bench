#!/usr/bin/env bash
# Create the Fluss catalog/tables and the Kafka-backed tables.
set -euo pipefail
cd "$(dirname "$0")/../docker"

run_sql() {
  echo ">> Running $1"
  # -i init.sql (re)creates the session-scoped Fluss catalog before each script.
  docker compose exec -T jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f "/opt/sql/$1"
}

run_sql 00_catalog_and_tables.sql
run_sql 22_kafka_tables.sql

echo ">> Tables ready."
