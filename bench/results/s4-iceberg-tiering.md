# S4 — Iceberg lakehouse tiering (live, 2026-06-24)

Fluss 0.9.1 tiering data to **Apache Iceberg**, proven end-to-end on the stack.

## S4a — Hot+cold: data tiers to Iceberg and is queryable  [DONE]
- Enabled datalake on the Fluss servers: `datalake.format=iceberg`,
  `datalake.iceberg.type=hadoop`, `datalake.iceberg.warehouse=/tmp/iceberg`.
- Created `tiered.events` with `table.datalake.enabled=true`, `freshness=30s`.
- Ran the **tiering service** (`fluss-flink-tiering` jar as a Flink job) →
  continuously moves Fluss log data into Iceberg.
- Ingested **500,000 rows**. After one freshness cycle the Iceberg warehouse held:
  - a snapshot (`snap-*.avro`), manifest (`*-m0.avro`), `v2.metadata.json`
  - **4 Parquet data files** (one per bucket), ~18 MB total
- `SELECT COUNT(*) FROM events` (batch) returned **500,000** — reading the tiered
  data. Note: batch full-scan only works BECAUSE datalake is enabled; a plain Fluss
  table rejects it ("point queries on primary key ... only").

## What it took (real gotchas — all now fixed in compose)
1. Fluss image ships an `iceberg` plugin but **no Hadoop**; the hadoop catalog needs
   `org.apache.hadoop.*` + `commons-logging`. Mounted hadoop-client-api/runtime 3.3.6
   + commons-logging into `/opt/fluss/plugins/iceberg` on BOTH servers.
2. The **Flink** side (tiering job + reads) needs the same Hadoop jars on its classpath
   → mounted into `/opt/flink/lib`.
3. The Iceberg warehouse dir is created `root:root 755` by the Fluss server; the Flink
   TM (uid 9999) then fails `Mkdirs ... data/__bucket=2`. Fix: make the warehouse
   world-writable (chmod 777). (For the repo, flink-init / an init step handles this.)

## S4b — Fluss tiered read vs Kafka full replay  [DONE — surprising result]
Bootstrap: read all 500,000 rows from the beginning (batch COUNT), same data both sides.

| Path | Job duration | Source | Notes |
|------|--------------|--------|-------|
| Kafka replay  | **1.6 s** | broker log from offset 0 | sequential log scan |
| Fluss tiered  | **4.2 s** | Iceberg Parquet | catalog + Parquet + Hadoop FS overhead |

**Honest, counter-narrative result:** at this scale Kafka replay was *faster* (1.6s vs
4.2s). The naive "Fluss wins cold reads" story does NOT hold for raw small-scale replay
speed — Kafka's sequential log read is hard to beat, and the Iceberg path pays catalog +
Parquet + Hadoop-FS startup overhead.

Where Fluss's tiering actually wins is **cost and decoupling, not replay latency**:
- Cold data lives in **cheap object storage as open Iceberg** (we saw ~18MB Parquet for
  500k rows), off the TabletServer's hot disk and entirely off the streaming serving path.
- That cold data is queryable directly by **Flink / Spark / Trino / StarRocks** without
  replaying through a broker at all — Kafka can't do that; its log is a closed format on
  broker disk (tiered-storage helps cost but it's still Kafka-internal, not open tables).
- Kafka full replay puts load on the **broker** (we measured broker CPU ~70% during the
  streaming replay, S4b-kafka-replay row in resources.csv); Fluss serves cold reads from
  object storage, sparing the serving tier.
- At large scale / many concurrent consumers, the broker-replay cost compounds while
  object-store reads scale out — but we did NOT prove that crossover on the laptop; we
  only show the architecture and the small-scale numbers, which favor Kafka on latency.

### Real Fluss bug found (worth reporting)
The **streaming** union read of the datalake table (Iceberg snapshot + live log) crashed
the TaskManager repeatedly with, in the Fluss writer path:
`IndexOutOfBoundsException: capacity: 131072, index: 131072` in
`MemoryLogRecordsArrowBuilder.append` (the 128KB Arrow page). The **batch** read of the
same table is stable (used above). This is an incubating-project stability rough edge,
not a config issue — exactly the kind of maturity gap the post should call out.

## Cost-relevant observation
The tiered Parquet (~18MB for 500k rows w/ 48-byte payload) is Fluss's *long-term*
storage footprint — columnar + compressed — vs Kafka retaining the raw log on broker
disk. The architectural point: with tiering, cold data leaves the hot serving path
(TabletServer local disk / broker) and lands in cheap object storage as open Iceberg,
readable by Flink/Spark/Trino without going through the streaming broker at all.
