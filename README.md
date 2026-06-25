# Apache Fluss vs Kafka — a hands-on benchmark with Flink 2.2

Is Apache Fluss a "Kafka killer"? This repo is the reproducible test bed behind the
Medium post — it drives **Fluss 0.9.1** and **Kafka 3.9 (KRaft)** with **Flink 2.2.1**,
using both Flink SQL and the DataStream API, and measures the things that actually
matter: **delta join vs stream-stream join checkpoint state, savepoint size, recovery,
CPU/memory, throughput, lookup joins, partial updates, Iceberg tiering, and the
Kafka-protocol bridge.**

Everything runs locally with `docker compose up`. The headline result:

> On the same 1.5M-key INNER join, Flink stream-stream state was **~87 MB**; with Fluss
> **delta join** it was **~48 KB**, and with a **lookup join** **~12.5 KB**. The savepoint
> went from **242 MB** to **28 KB**. The join state moves out of Flink and into Fluss.

![Delta join keeps checkpoints flat as join state grows](post/img/01_delta_join_cardinality_sweep.png)

## TL;DR verdict

Fluss is **not** a Kafka killer — it's a **complement** that wins decisively in one lane:
being the state/serving tier for Flink. See the full write-up in
[`post/fluss-vs-kafka.md`](post/fluss-vs-kafka.md) (or the styled
[`post/fluss-vs-kafka.html`](post/fluss-vs-kafka.html)).

![scorecard](post/img/05_scorecard.png)

## Versions (verified)

| Component | Version |
|---|---|
| Apache Fluss | `0.9.1-incubating` (latest as of mid-2026; Apache incubating) |
| Apache Flink | `2.2.1` (delta join needs ≥ 2.1.2 / 2.2) |
| Apache Kafka | `3.9` (KRaft, no ZooKeeper) |
| Fluss Flink connector | `org.apache.fluss:fluss-flink-2.2:0.9.1-incubating` |

## Quick start

```bash
# 1. download the connector jars Flink's SQL client needs (kept out of git)
./scripts/fetch-connectors.sh

# 2. bring up Fluss + Kafka + Flink 2.2 + Prometheus/Grafana (+ MinIO, ZooKeeper)
./scripts/up.sh

# 3. build the DataStream jobs
./scripts/build-jobs.sh

# 4. create the Fluss catalog + tables
./scripts/init-tables.sh
```

Endpoints: Flink UI `:8082`, Grafana `:3000`, Prometheus `:9090`, MinIO `:9101`.

> **Note:** every `sql-client.sh -f` needs `-i /opt/sql/init.sql` to (re)create the
> session-scoped Fluss catalog. See [`docs gotchas`](bench/FINDINGS.md).

## Reproduce the headline experiments

```bash
# delta join vs stream-stream vs lookup — checkpoint + CPU/mem (the 87MB -> 48KB -> 12.5KB result)
CARD=300000 PAYLOAD=400 ./scripts/run-state-comparison.sh

# consistency: do all 3 joins produce the same rows?
N=1000 ./scripts/run-consistency-check.sh

# delta-join checkpoint vs key cardinality (the money chart, 3x -> 261x)
./scripts/sweep-delta-join.sh

# regenerate the charts from results CSVs
python3 -m venv .chartenv && .chartenv/bin/pip install matplotlib pandas
.chartenv/bin/python scripts/make_charts.py
```

## What's measured (and where the numbers live)

All raw per-scenario findings are in [`bench/results/`](bench/results/) — see its
[README](bench/results/README.md) for the scorecard and index. Highlights:

| Scenario | Result | Winner |
|---|---|---|
| Stateful join state (cardinality sweep) | checkpoint 3× → **261×** smaller (1k→200k keys) | **Fluss** |
| 100 MB-load 3-way state | 87 MB → 48 KB → 12.5 KB | **Fluss** |
| Savepoint size / recovery | 242 MB → 28 KB; recovery reload vs nothing | **Fluss** |
| Ingest throughput | Kafka ~11% faster | Kafka |
| Bootstrap / replay (500k) | Kafka 1.6 s vs Fluss-tiered 4.2 s | Kafka |
| Lookup joins (INNER + LEFT, async) | enrich w/o external KV, no Flink-held dim | **Fluss** |
| Partial updates | multi-source column merge | **Fluss** (Kafka can't) |
| Iceberg tiering | open-format cold tier | **Fluss** (Kafka can't) |
| Kafka wire-protocol drop-in | connects but not usable yet | Neither (WIP) |
| Duplicates / retractions | a Flink + sink property, not a Fluss one | — |

## Layout

```
docker/        docker-compose stack + Flink/Prometheus/Grafana config
flink-jobs/    Java DataStream jobs (Maven): Fluss read/write + Kafka interop
sql/           Flink SQL scenarios (catalog, delta join, lookup, partial update, tiering)
scripts/       up / build / init + the measurement & chart scripts
bench/         FINDINGS.md (verified facts + sources), SCENARIOS.md, results/
post/          the Medium article (md + html) and chart images
eks/           Part 2: scale-out plan + Kubernetes/Helm templates (scaffold)
```

## Part 2 — at scale on EKS

The single-node stack proves the *mechanics and ratios* but can't show recovery-time
divergence at multi-GB state, the throughput ceiling, sustained stability, or cost.
[`eks/`](eks/) holds the scale-out plan and Kubernetes/Helm templates for Part 2.

## License

Apache 2.0 — see [LICENSE](LICENSE). This is an independent benchmark; Apache Fluss,
Flink, Kafka, and Iceberg are trademarks of the Apache Software Foundation.
