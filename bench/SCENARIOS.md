# Benchmark scenario matrix — "Kafka killer or complement?"

End goal: a decision-grade matrix of latency / throughput / resource-cost tradeoffs
across realistic use cases, so the post can say *when* Fluss wins, *when* it
complements Kafka, and *when* Kafka still wins. Cost axis = resource-proxy (measured
CPU, memory, network, storage/state from Prometheus + `docker stats` + Flink REST),
not cloud dollars.

Status legend: [x] done & measured · [~] partially run · [ ] to build

## VERDICT SUMMARY (all scenarios run 2026-06-24)
| # | Scenario | Result | Who wins |
|---|----------|--------|----------|
| S1 | Delta vs stream-stream join state | 200k keys: 54KB vs 14.2MB checkpoint (~260x) | **Fluss** (big) |
| S2 | Ingest throughput (2M rows) | Kafka 98.6k vs Fluss 88.7k rec/s | Kafka (~11%) |
| S3 | Column pruning | pushdown proven in plan; magnitude unmeasured at scale | Fluss (qual.) |
| S4a| Iceberg tiering hot+cold | 500k rows tiered to Iceberg, queried (batch) | **Fluss** (Kafka can't) |
| S4b| Bootstrap/replay 500k | Kafka 1.6s vs Fluss-tiered 4.2s | Kafka (latency); Fluss (cost/decoupling) |
| S5 | Kafka wire-protocol drop-in | connects but METADATA fails; not usable | Neither (Fluss WIP) |

Net: Fluss decisively wins the **stateful-join / state-offload** use case (S1) and
unlocks **open-format lakehouse tiering** Kafka structurally lacks (S4a). Kafka still
wins **raw ingest + simple replay latency** (S2, S4b) and **client ecosystem** (S5).


## S1 — Stateful join: delta join vs stream-stream join  [x]
- Metric: Flink checkpoint state size, checkpoint duration, recovery.
- Result: 200k keys → DeltaJoin ~54KB vs stateful Join ~14.2MB (~260x). Toy 6-row
  → ~1.2x. Gap scales with key cardinality. (bench/results/scaled-delta-vs-stream.md)
- TODO exhaustive: add TM CPU/heap from Prometheus during each run; multiple
  cardinalities (1k / 50k / 500k) to plot the curve; recovery-time-from-checkpoint.

## S2 — Ingest throughput + end-to-end latency: Fluss vs Kafka  [ ]
- Same datagen feed → (a) Fluss log table, (b) Kafka topic, via Flink.
- Metrics: sustained records/s at fixed parallelism; p50/p95/p99 end-to-end latency
  (event ts → visible to a tail reader); TM CPU + network; bytes-on-disk per record.
- Hypothesis: comparable ingest; Fluss heavier write path (Arrow encode + remote tier)
  but cheaper reads (next scenario).

## S3 — Projection / column pruning read cost  [ ]
- Wide row (~30 cols); read 2 cols vs all cols.
- Fluss (columnar Arrow, server-side projection pushdown) vs Kafka (ships full record,
  client-side filter).
- Metrics: read throughput, bytes transferred, consumer CPU. Fluss claims ~10x at
  ~90% pruning — verify the curve at a few prune ratios.

## S4 — State rebuild / cold read (the tiered story)  [ ]  ← uses ICEBERG
Two parts (user asked for both):
- **S4a Fluss hot+cold union read**: enable Iceberg tiering; let old data tier to
  Iceberg, keep recent data in the Fluss log; run one query spanning both; time it +
  resource cost. Shows Fluss serving cold+hot as one table.
- **S4b Fluss tiered vs Kafka full replay**: bootstrap a stateful job that must read
  EVERYTHING from the beginning.
  - Fluss: reads cold history from Iceberg (columnar, parallel, cheap) + tail from log.
  - Kafka: must replay every record from offset 0 through the broker.
  - Metrics: time-to-rebuild, bytes read, broker/server CPU, network. This is where
    Fluss's "don't replay the whole log through the broker" advantage should show.

## S5 — Kafka-protocol compatibility (complement story)  [ ]
- Point a stock Kafka client/console-consumer at the `fluss-kafka` protocol port;
  confirm produce/consume works unmodified. Qualitative: what works, what doesn't.

## Cross-cutting resource capture (exhaustive)
For every scenario, snapshot during steady state:
- Flink: TM CPU load, heap used, managed mem, checkpoint size/dur (Prometheus + REST).
- Fluss: TabletServer/Coordinator container CPU+mem (`docker stats`), bytes in MinIO.
- Kafka: broker container CPU+mem, topic bytes on disk.
- Store raw CSV per run under bench/results/, summarize in a markdown table.

## Iceberg tiering recipe (verified from docs, to wire up)
- Fluss server.yaml (coordinator + tablet): `datalake.enabled: true`,
  `datalake.format: iceberg`, `datalake.iceberg.type: hadoop`,
  `datalake.iceberg.warehouse: s3://fluss/iceberg` (or /tmp/iceberg local).
- Per-table: `'table.datalake.enabled' = 'true'` (+ optional freshness/auto-compaction).
- Tiering service = separate Flink job:
  `flink run fluss-flink-tiering-0.9.1-incubating.jar --datalake.format iceberg
   --datalake.iceberg.type hadoop --datalake.iceberg.warehouse <wh>`
  with `fluss.bootstrap.servers` set.
- Jars needed: `fluss-lake-iceberg-0.9.1-incubating.jar` (+ Hadoop classpath for the
  hadoop catalog). Iceberg catalog types also support rest/glue/nessie/jdbc/hive.
- NOTE: only full-table batch scans / datalake-enabled tables support arbitrary batch
  reads (recall the "point queries only" limitation for non-datalake tables).
