# Verified Fluss facts (research, 2026-06-24)

All version/API facts below were verified against the GitHub API, Maven Central,
Docker Hub, and the Fluss docs source (`website/docs/...` on `apache/fluss@main`)
on 2026-06-24. Sources listed at bottom.

## Versions (ground truth)
- **Latest Fluss release: `0.9.1-incubating`** (published 2026-05-04). Prior:
  `0.9.0-incubating` (2026-03-02), `0.8.0-incubating` (2025-11-06). Still **Apache
  incubating** (not yet a TLP).
- **Flink 2.2 connector exists**: Maven `org.apache.fluss:fluss-flink-2.2:0.9.1-incubating`
  (jar `fluss-flink-2.2-0.9.1-incubating.jar`). Published Flink connectors:
  `fluss-flink-1.18 / 1.19 / 1.20 / 2.1 / 2.2`.
- Docker images: `apache/fluss:0.9.1-incubating`; Flink `flink:2.2.1-scala_2.12-java17`.
- Other artifacts of note: `fluss-kafka` (Kafka wire-protocol compat layer),
  `fluss-spark-3.4/3.5`, `fluss-lake-paimon/iceberg/lance`, `fluss-flink-tiering`.

## Architecture
- Two table types: **Log Tables** (append-only) and **PrimaryKey Tables** (updatable, upsert).
- Columnar log format (**Arrow**, default `table.log.format=ARROW`, ZSTD); supports
  projection/column pruning. Alt formats: `INDEXED`, `COMPACTED`.
- **CoordinatorServer** + **TabletServer** processes. **Still requires ZooKeeper**
  (`zookeeper.address`) in 0.9.1 — contrast with Kafka KRaft (ZK-free).
- **Remote storage required** (`remote.data.dir`, e.g. S3/MinIO) for tiering; local
  `data.dir` for hot data. `table.log.tiered.local-segments` (default 2).
- Lakehouse tiering: `table.datalake.enabled`, `table.datalake.format` =
  paimon|iceberg|lance, `table.datalake.freshness` (default 3min).

## LIVE-VERIFIED on the running stack (2026-06-24)

Booted the full docker-compose stack (9 containers healthy) and ran real jobs.
Confirmed end-to-end, not just from docs:
- Stack boots: ZK + MinIO + Fluss coordinator/tablet + Kafka KRaft + Flink 2.2 + Prom/Grafana.
- Fluss catalog (`'type'='fluss'`) + DDL create; **write 1,000,000 rows** into a log
  table and **read them back** (batch COUNT = 1000000); PK upsert works (3 users).
- **DELTA JOIN PROVEN**: `EXPLAIN INSERT INTO <upsert sink> SELECT ... INNER JOIN`
  produces a `DeltaJoin` node, and the live job graph runs
  `DeltaJoin[5] -> Calc -> ConstraintEnforcer -> Sink` (default strategy AUTO).
- The connector jars must be on the Flink **SQL classpath** (`/opt/flink/lib`), not
  just `usrlib`: `fluss-flink-2.2-0.9.1-incubating.jar` (66MB) +
  `flink-sql-connector-kafka-5.0.0-2.2.jar`. (scripts/fetch-connectors.sh)

### Exact delta-join requirements (learned by making FORCE fail, then fixing)
The optimizer key is **`table.optimizer.delta-join.strategy`** (AUTO|FORCE|NONE,
default AUTO). It silently falls back to a stateful join unless ALL hold:
1. Both inputs are Fluss PK tables.
2. Join key = `bucket.key` AND a **prefix of the PK**. Fluss only emits a prefix
   index when PK length > bucket-key length (see FlinkCatalog: `isPrefixList`).
   => `orders_pk` PK must be `(user_id, order_id)`, NOT `(order_id, user_id)`.
3. Inputs **insert-only** (changelogMode `[I]`): set `table.merge-engine=first_row`
   (or for CDC, `table.delete.behavior=IGNORE|DISABLE`). PK tables default to
   `[I,UA,D]`, which the planner rejects.
4. **INNER JOIN only.**
5. Pattern must be `source -> join -> UPSERT SINK`. A bare `EXPLAIN SELECT` (no
   sink) will NEVER show DeltaJoin — must be `EXPLAIN INSERT INTO sink SELECT ...`.
6. Fluss supports only ONE index per table (the prefix key); general secondary
   indexes are "planned for future releases" — so multi-key delta joins are out.

### Newly found Fluss limitations (live)
- **Batch reads are restricted**: `SELECT ... ORDER BY`/full scan throws
  "Fluss only support queries on table with datalake enabled or point queries on
  primary key when it's in batch execution mode." Full analytical batch scans need
  `table.datalake.enabled` (lakehouse tiering). Streaming reads are the norm.
- SQL client `-f` script mode is finicky for rendering SELECT results; catalogs are
  session-scoped so every `-f` invocation needs `-i /opt/sql/init.sql` to recreate
  the Fluss catalog (tables persist in Fluss; the catalog definition does not).
- **Flink 2.2 uses `config.yaml` (nested), NOT the legacy flat `flink-conf.yaml`.**
  Mounting flink-conf.yaml is silently ignored (cost us: slots stuck at 1, no
  checkpoint/metric config). Do NOT override `env.java.opts.all` in a mounted
  config.yaml — the docker entrypoint round-trips the file and truncates the long
  --add-opens line, which then breaks the sql-client / `flink run` launcher JVMs
  ("Unable to parse --add-opens"). Let the image set its own Java opts.
- **TaskManager memory**: the Fluss client's shaded-Netty allocator needs ample
  direct memory. A 2560m TM OOMs ("Direct buffer memory") under write load; bumped
  to 4096m process + explicit task/framework off-heap. Batch-shuffle reads also need
  framework off-heap headroom.
- **Submit DataStream jobs via the REST API, not `flink run`** on this image: the CLI
  client JVM inherits the mangled java opts and fails to launch. scripts/run-datastream.sh
  uploads the jar and POSTs /jars/{id}/run instead. (Compiles + submits + runs:
  DeltaJoin/Source/Sink operators confirmed in the live job graph.)
- The named checkpoint volume is root-owned on creation; Flink runs as uid 9999 ->
  "Failed to create directory for shared state" once checkpointing is enabled. A
  one-shot `flink-init` service chowns it. (Surfaced only AFTER the config.yaml fix
  actually turned checkpointing on — earlier it was silently disabled.)

## Delta Join (headline feature) — verified from delta-joins.md + live run
- Streaming join operator (Flink 2.1+) that turns "query state" into "query the Fluss
  source table" via an **index-key (prefix-key) lookup**, so the large join state lives
  in Fluss PK-table indexes, NOT in Flink RocksDB managed state.
- Effect: **much smaller Flink checkpoints**, shorter checkpoint duration, faster
  recovery, little/no state bootstrap. This is the core claim the benchmark tests.
- **Requirements**: both tables have PRIMARY KEY; join key must be the prefix/bucket key
  (`'bucket.key' = '<join_col>'`) and part of the PK; join key in the equi-condition.
- **Join type support**: **INNER JOIN only.** On Flink 2.1 INSERT-ONLY sources required
  (`table.merge-engine=first_row`); on Flink 2.2, CDC sources allowed if deletes
  suppressed (`table.delete.behavior = IGNORE | DISABLE`).
  **LEFT / RIGHT / FULL OUTER are explicitly NOT supported** — even on 2.2.
  => Honest post point: "left delta join" is not a thing yet; outer joins fall back to
     regular stream-stream join with full Flink state.
- **Flink SQL only** (planner optimization, transparent). Not a DataStream API feature.
- Flink <= 2.1.1 had a known delta-join bug; fixed in 2.1.2 / 2.2.

## DataStream API (datastream.mdx)
- `FlussSource.<T>builder()` — setBootstrapServers/setDatabase/setTable/
  setDeserializationSchema; optional setProjectedFields, setStartingOffsets (OffsetsInitializer
  .earliest()/.latest()/.full()/.timestamp()), setScanPartitionDiscoveryIntervalMs.
  Deserializers: `RowDataDeserializationSchema`, `JsonStringDeserializationSchema`.
  PK tables emit changelog (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE); log tables INSERT-only.
- `FlussSink.<T>builder()` — setBootstrapServers/setDatabase/setTable/setSerializationSchema;
  setShuffleByBucketId (default true). Serializers: `RowDataSerializationSchema`
  (ctor flags isAppendOnly, ignoreDelete), `JsonStringSerializationSchema`; custom returns
  `RowWithOp` (UPSERT|DELETE).

## Key option keys (options.md)
- DDL/table: `bucket.num`, `bucket.key`, `table.log.ttl` (7d), `table.log.format` (ARROW),
  `table.merge-engine` (last_row|first_row|versioned|aggregation), `table.delete.behavior`
  (ALLOW|IGNORE|DISABLE), `table.changelog.image` (FULL|WAL), `table.datalake.*`.
- Read: `scan.startup.mode` (full|earliest|latest|timestamp), `scan.startup.timestamp`.
- Lookup: `lookup.async` (true), `lookup.cache` (NONE|PARTIAL), `lookup.partial-cache.*`.
- Write: `sink.distribution-mode` (AUTO|NONE|BUCKET|PARTITION_DYNAMIC), `sink.ignore-delete`,
  `client.writer.acks` (all).
- Catalog: `'type'='fluss'`, `'bootstrap.servers'='coordinator-server:9123'`.

## Interop
- Kafka<->Fluss done via standard Flink Kafka connector + Fluss connector (no special
  bridge needed). `fluss-kafka` artifact provides Kafka-protocol compatibility (clients can
  talk to Fluss using Kafka APIs) — separate from the Flink path.

## SCALED headline result (200k keys, live 2026-06-24)
Same INNER JOIN query, AUTO vs NONE strategy, on 200k-user / 200k-order Fluss PK tables:
- Delta join: `DeltaJoin` operator, checkpoint state **~54 KB**.
- Stream-stream: stateful `Join`, checkpoint state **~14.2 MB**.
- **≈260x smaller checkpoint for delta join.** At 6 rows the gap was ~1.2x (34KB vs
  42KB); the gap scales with key cardinality — that's the whole thesis, now measured.
  Detail: bench/results/scaled-delta-vs-stream.md.

## CRITICAL infra finding: Arrow needs JVM flags on stock Flink image
- The official Fluss quickstart uses the **prebaked `apache/fluss-quickstart-flink`
  image**, which bundles the connector, flink-faker, S3, AND the JVM flags Arrow needs.
- We use the **stock `flink:2.2.1` image** (more transparent — every dep is visible),
  so we must supply Arrow's flags ourselves. Without them, Fluss sources fail at runtime:
  `Failed to initialize MemoryUtil ... start Java with --add-opens=java.base/java.nio=ALL-UNNAMED`.
- Gotcha within the gotcha: setting `env.java.opts.all` via a multiline FLINK_PROPERTIES
  env collapses spaces → flags concatenate into one malformed token → silently ignored.
  Fix: put a QUOTED, space-separated `env.java.opts.all` directly in config.yaml (short
  enough to avoid entrypoint truncation): --add-opens java.base/{java.nio,java.lang,
  java.util,java.lang.reflect}=ALL-UNNAMED plus -Dio.netty.tryReflectionSetAccessible=true.
- And: bind-mounted config.yaml changes need `docker compose up -d --force-recreate`;
  a plain `up -d` does NOT restart the container for a mounted-file-only change.
- Direct-buffer "OOM" errors at high cardinality were mostly this Arrow init failure
  cascading; once flags were correct, a 6g TM with task.off-heap=1536m ran 200k-key
  joins fine (one job at a time on a single TM).

## Fluss's OWN benchmarks & samples (researched 2026-06-24)
- **No head-to-head Kafka benchmark harness in the repo.** Fluss ships only JMH
  micro-benchmarks in `fluss-jmh/src/test/java/org/apache/fluss/jmh/`:
  `LogScannerBenchmark` (scan 1,000 records, 1 bucket), `ArrowReadableChannelBenchmark`,
  `ArrowWritableChannelBenchmark` (Arrow encode/decode). These measure internal
  read/scan/encoding throughput, NOT a Fluss-vs-Kafka comparison. => OUR repo fills
  that gap (a real side-by-side on Flink 2.2).
- **Published headline claim: "10x read throughput" from column pruning** ("How Apache
  Fluss Achieves True Pruning in Streaming Storage", blog, Apr 2026): pruning ~90% of
  columns yields ~10x read throughput, scaling ~linearly with prune ratio. Mechanism:
  Arrow IPC columnar + zero-copy server-side projection, vs Kafka shipping all fields
  and filtering client-side. Treat as a vendor claim; setup not fully disclosed.
- **`fluss-kafka` = a Kafka wire-protocol SERVER plugin** (`KafkaProtocolPlugin`
  implements `NetworkProtocolPlugin`). Handlers cover a broad Kafka API surface:
  PRODUCE, FETCH, METADATA, LIST_OFFSETS, consumer groups (JoinGroup/Heartbeat/
  OffsetCommit/OffsetFetch/...), transactions (InitProducerId/AddPartitionsToTxn/
  EndTxn), and topic admin (CreateTopics/DeleteTopics/CreatePartitions/AlterConfigs).
  => Existing Kafka clients (any language) can point at Fluss with no code change —
  this is the backbone of the "complement, migrate gradually" story, distinct from
  the Flink connector path. (Maturity/coverage vs real Kafka still unproven.)
- Quickstart docs: `website/docs/quickstart/{flink,lakehouse,security}.md` — Flink SQL
  driven; no DataStream quickstart, no perf/bench quickstart.

## Sources
- https://api.github.com/repos/apache/fluss/releases
- https://search.maven.org/solrsearch (g:org.apache.fluss)
- https://hub.docker.com/v2/repositories/apache/fluss/tags , library/flink (2.2)
- apache/fluss@main: website/docs/engine-flink/{delta-joins,datastream,options,getting-started}.md
- https://fluss.apache.org/docs/
