# S5 — Fluss Kafka wire-protocol compatibility (live, 2026-06-24)

Question: can a **stock Kafka client** talk to Fluss unmodified? (The "complement /
gradual migration" story — point existing Kafka producers/consumers at Fluss.)

## Setup (what it took to even try)
- `fluss-kafka` is NOT in the Docker image; fetched `fluss-kafka-0.9.1-incubating.jar`
  (22 KB, thin) from Maven and mounted into `/opt/fluss/lib`.
- It needs Kafka client classes too → also mounted `kafka-clients-3.9.0.jar` (8.8 MB).
- Config: `kafka.enabled: true`, `kafka.listener.names: KAFKA`, and a
  `KAFKA://...:19092` entry in `bind.listeners`.
- Constraint discovered: **the Kafka listener can only run on the TabletServer, not
  the CoordinatorServer** ("Kafka protocol endpoints can only be enabled on
  TabletServers"). Moved it there.
- Result: TabletServer logs `Listening on ... port 19092 for KAFKA protocol`. The
  listener is up.

## Result: connects, but NOT usable by a stock client (yet)
Pointed Kafka 3.9.0's own `kafka-topics.sh`, `kafka-console-producer.sh`, and
`kafka-broker-api-versions.sh` at `tablet-server:19092`:
- The TCP connection succeeds — Fluss's `KafkaCommandDecoder` logs
  `New connection from ...` for every attempt.
- But every operation fails on **METADATA**:
  - `kafka-topics --create/--list`: *"Timed out waiting for a node assignment"*.
  - console producer: *"Topic compat not present in metadata after 15000 ms"*, then
    *"Bootstrap broker tablet-server:19092 (id: -1 rack: null) disconnected"*.
  - `broker-api-versions`: *"Request METADATA failed"*.
- The `id: -1` means the Metadata response doesn't advertise a usable broker node, so
  the client never proceeds to produce/fetch.

## Verdict
The Fluss Kafka protocol layer in 0.9.1 **accepts connections and has a broad set of
request handlers wired** (Produce/Fetch/Metadata/consumer-groups/txn/admin in the
source), but it is **not a drop-in for a stock Kafka client** — the Metadata/broker
discovery path doesn't yet satisfy a real Kafka 3.9 client, so produce/consume/admin
all time out. This matches the docs verbatim: *"Kafka protocol compatibility is still
in development."*

=> For the post: the migration-bridge story is **promising but not ready**. Today you
integrate via the Flink connector (proven, S1–S4), not by repointing Kafka clients.
This is a concrete maturity gap, demonstrated rather than asserted.
