# Part 2 — Fluss vs Kafka at scale on EKS

Goal: re-run the Part-1 scenarios on a real multi-node cluster to close the four gaps the
laptop could not measure. Part 1 proved the *mechanics and ratios* (delta join 87 MB→48 KB
checkpoint, 242 MB→28 KB savepoint, INNER-only delta, async INNER/LEFT lookup, Flink-owned
duplicate semantics). Part 2 proves what only scale + remote storage can show.

## The four experiments (each maps to a Part-1 "couldn't measure on a laptop")
| # | Experiment | Part-1 gap it closes | Headline metric |
|---|-----------|----------------------|-----------------|
| E1 | **Recovery at multi-GB state, savepoints on S3** | recovery time was ~7s for all (242MB hid behind startup) | restore seconds: stream-stream vs delta vs lookup as state grows 1→10→50 GB |
| E2 | **Throughput ceiling at parallelism** | single TM crashed on uncapped ingest | sustained records/s, Fluss vs Kafka ingest; delta/lookup join rate at N TMs |
| E3 | **Stability under sustained load** | tablet dropouts / heartbeat timeouts / Kafka OOM were single-node | hours of steady run, restart count, p99 checkpoint duration |
| E4 | **Cost** | no $/throughput axis | $/M-records ingested, $/GB-tiered (EC2 + S3 + EBS), MSK vs self-managed Fluss |

The signature chart for Part 2: **restore time vs state size**, where stream-stream climbs
and delta/lookup stay flat once the savepoint lives on S3 and is multi-GB.

## Cluster shape (starting point — tune to budget)
- **EKS** 1.30+, 2 node groups:
  - `core` (3 × m6i.xlarge): Fluss CoordinatorServer + TabletServers, ZooKeeper, Kafka (MSK or self-managed), operators.
  - `flink` (3–6 × m6i.2xlarge): Flink TaskManagers (the scalable tier).
- **S3** buckets: `fluss-remote-data` (Fluss tiering/remote log), `flink-checkpoints`, `flink-savepoints`, `iceberg-warehouse`.
- **EBS gp3** for TabletServer local hot data + RocksDB local dirs.
- IRSA (IAM Roles for Service Accounts) so pods get S3 access without static keys.

## Components & how they deploy
1. **Fluss** — official Helm chart (`helm/` in apache/fluss). Values: coordinator + N
   tabletservers, `remote.data.dir=s3://fluss-remote-data`, `datalake.format=iceberg`,
   `datalake.iceberg.warehouse=s3://iceberg-warehouse`, ZooKeeper subchart or a ZK statefulset.
   Mount the Iceberg hadoop + commons-logging jars (Part-1 finding) OR use a baked image.
2. **Flink** — **Flink Kubernetes Operator** (helm: flink-operator). FlinkDeployment CRs
   for the cluster; jobs as FlinkSessionJob CRs. config.yaml carries the Arrow `--add-opens`
   JVM flags (Part-1 finding) + S3 checkpoint/savepoint dirs + the Fluss/Iceberg/Kafka jars
   in the image's /opt/flink/lib (bake an image; don't bind-mount at scale).
3. **Kafka** — MSK (managed) for the baseline, OR Strimzi for an apples-to-apples self-managed
   cost comparison. KRaft mode.
4. **Observability** — kube-prometheus-stack; Flink Prometheus reporter (port 9249) scraped
   via PodMonitor; Grafana dashboards reused from Part 1 (checkpoint size, TM mem, tablet CPU).

## Key differences from Part 1 (lessons already learned)
- **Bake a Flink image** with all jars (fluss-flink-2.2, flink-sql-connector-kafka,
  fluss-lake-iceberg, hadoop-client-api/runtime, commons-logging) + the Arrow JVM opts in
  config.yaml. No bind-mounts, no `--force-recreate` dance.
- **S3, not local disk** for checkpoints/savepoints — this is what makes E1 meaningful
  (242 MB→GB reload over network, not local).
- **Give TaskManagers real memory** (Part 1: Fluss Arrow/Netty is direct-memory hungry;
  undersized TMs manifest as flaky tablet registration + heartbeat timeouts, not clean OOM).
- **Iceberg requires single-field PK** on datalake-enabled tables (Part-1 finding) — design
  scaled tables accordingly, or keep composite-PK tables non-datalake.

## Run order
1. `eks/manifests/00-namespace-irsa.yaml` ... apply infra (see manifests/README).
2. Deploy Fluss (Helm) + Flink operator (Helm).
3. Build/push the baked Flink image (`eks/Dockerfile.flink`).
4. Submit FlinkSessionJob CRs from `eks/sql/` (reuse Part-1 SQL, S3 dirs).
5. E1: load 1→10→50 GB state, savepoint to S3, kill, restore, time it (the money chart).
6. E2/E3/E4: scale TMs, sustained ingest, capture cost from instance + S3 metrics.

## Status
SCAFFOLD ONLY — manifests here are templates to adapt to your account (VPC, subnets,
bucket names, IAM). Not yet executed. This is the Part-2 starting point.
