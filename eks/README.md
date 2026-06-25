# EKS — Part 2 (scale-out) scaffold

Templates to run the Part-1 scenarios on a real EKS cluster and close the four gaps the
laptop couldn't measure (recovery at multi-GB state on S3, throughput at parallelism,
stability under sustained load, cost). **Scaffold only — adapt placeholders to your
account; not yet executed.** Full design in [PLAN.md](PLAN.md).

## Files
- `PLAN.md` — the four experiments, cluster shape, run order, Part-1 lessons to carry over.
- `Dockerfile.flink` — baked Flink image (all jars + Arrow JVM opts + S3 plugin). No
  bind-mounts at scale.
- `fluss-values.yaml` — Helm values for Fluss (coordinator + tablet servers + ZK), remote
  data + Iceberg tiering on S3.
- `manifests/10-flink-cluster.yaml` — FlinkDeployment (session cluster), checkpoints +
  savepoints on S3, real TM memory.
- `manifests/20-job-deltajoin.yaml` — FlinkSessionJob + the E1 recovery procedure.
- `sql/` — reuse Part-1 SQL (`sql/90_state_comparison.sql`) pointed at S3-backed tables.

## Placeholders to replace
`<ECR_REPO>` (image registry), `<BUCKET>` (S3 bucket prefix), `<ROLE>` / IRSA role ARNs,
VPC/subnet/region specifics.

## Quick path
1. Create S3 buckets + IRSA role (S3 RW) bound to `flink` and Fluss service accounts.
2. `docker build -f eks/Dockerfile.flink -t <ECR_REPO>/fluss-flink:2.2.1-0.9.1 . && docker push ...`
3. `helm install flink-operator flink-operator/flink-kubernetes-operator -n fluss-bench`
4. `helm install fluss fluss/fluss -n fluss-bench -f eks/fluss-values.yaml`
5. `kubectl apply -f eks/manifests/10-flink-cluster.yaml`
6. Upload the job jar to S3, `kubectl apply -f eks/manifests/20-job-deltajoin.yaml`
7. Run E1–E4 per PLAN.md; reuse Part-1 Grafana dashboards + metric-capture approach.

## The Part-2 money chart
**Restore time vs state size (1 → 10 → 50 GB), savepoints on S3:** stream-stream should
climb into minutes while delta/lookup stay flat — the production proof of "state in Fluss,
not Flink". Part 1 couldn't show this because 242 MB on local disk hid behind ~7 s of
job-startup overhead.
