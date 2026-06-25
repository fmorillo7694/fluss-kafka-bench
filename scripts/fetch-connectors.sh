#!/usr/bin/env bash
# Download the connector jars the Flink SQL client needs on its classpath.
# These are mounted into /opt/flink/lib by docker-compose. Kept out of git
# (see .gitignore) because they're large and reproducible.
set -euo pipefail
cd "$(dirname "$0")/../docker/flink-lib"

FLUSS_VER=0.9.1-incubating
KAFKA_CONN=5.0.0-2.2
BASE=https://repo1.maven.org/maven2

fetch() { # url filename
  if [ -f "$2" ]; then echo ">> have $2"; else echo ">> fetching $2"; curl -fsSL -o "$2" "$1"; fi
}

fetch "$BASE/org/apache/fluss/fluss-flink-2.2/$FLUSS_VER/fluss-flink-2.2-$FLUSS_VER.jar" \
      "fluss-flink-2.2-$FLUSS_VER.jar"
fetch "$BASE/org/apache/flink/flink-sql-connector-kafka/$KAFKA_CONN/flink-sql-connector-kafka-$KAFKA_CONN.jar" \
      "flink-sql-connector-kafka-$KAFKA_CONN.jar"

echo ">> connector jars ready:"; ls -lh ./*.jar

# --- Iceberg tiering (S4): lake connector, tiering job, + Hadoop for the hadoop catalog ---
cd "$(dirname "$0")/../docker/flink-lib"
fetch "$BASE/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VER/fluss-lake-iceberg-$FLUSS_VER.jar" "fluss-lake-iceberg-$FLUSS_VER.jar"
fetch "$BASE/org/apache/fluss/fluss-flink-tiering/$FLUSS_VER/fluss-flink-tiering-$FLUSS_VER.jar" "fluss-flink-tiering-$FLUSS_VER.jar"
mkdir -p ../hadoop-libs && cd ../hadoop-libs
fetch "$BASE/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar" "hadoop-client-api-3.3.6.jar"
fetch "$BASE/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar" "hadoop-client-runtime-3.3.6.jar"
fetch "$BASE/commons-logging/commons-logging/1.2/commons-logging-1.2.jar" "commons-logging-1.2.jar"
echo ">> Iceberg + Hadoop jars ready"

# --- Kafka wire-protocol layer (S5) ---
cd "$(dirname "$0")/../docker/flink-lib"
fetch "$BASE/org/apache/fluss/fluss-kafka/$FLUSS_VER/fluss-kafka-$FLUSS_VER.jar" "fluss-kafka-$FLUSS_VER.jar"
fetch "$BASE/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" "kafka-clients-3.9.0.jar"
echo ">> Kafka protocol jars ready"
