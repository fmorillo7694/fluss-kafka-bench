#!/usr/bin/env bash
# Build the Flink benchmark jar. Output lands in flink-jobs/target and is
# mounted into the Flink containers at /opt/flink/usrlib.
set -euo pipefail
cd "$(dirname "$0")/../flink-jobs"

if command -v mvn >/dev/null 2>&1; then
  mvn -q clean package
else
  echo ">> Maven not found locally; building inside a Maven container ..."
  docker run --rm -v "$PWD":/work -v "$HOME/.m2":/root/.m2 -w /work \
    maven:3.9-eclipse-temurin-17 mvn -q clean package
fi

echo ">> Built: $(ls -1 target/*.jar | tail -1)"
