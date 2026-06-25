# S8c — Kafka stream + lookup join into Fluss dimension (live, 2026-06-25)

THE realistic hybrid: **Kafka as the bus, Fluss as the state/serving tier.** A
Kafka-backed orders stream enriched, per event, against a Fluss PK `users` dimension
via `FOR SYSTEM_TIME AS OF` — no external KV store, no dimension held in Flink state.
(Earlier S8b used a Fluss-backed driving stream; this one uses Kafka, which is the
pattern people actually deploy.)

## Topology
```
Kafka topic 'orders_hybrid'  ──▶  Flink lookup join  ──▶  Kafka topic 'enriched_hybrid'
                                        │ FOR SYSTEM_TIME AS OF
                                        ▼
                              Fluss PK table  hybrid.users   (async lookup, indexed)
```

## Measured result
Fluss dimension preloaded with user_id 1,2 (user 3 deliberately absent). Produced
3 orders (users 1,2,3) to Kafka. The join output (one row per order):

    {"order_id":100,"user_id":1,"amount":10,"name":"Ada"}     # hit
    {"order_id":200,"user_id":2,"amount":20,"name":"Borg"}    # hit
    {"order_id":300,"user_id":3,"amount":30,"name":null}      # miss, clean LEFT null

- **One output event per input order** (no retraction storm — same clean property as
  S8b, now with a Kafka source). A miss is just `name=null`, point-in-time.
- The Fluss dimension is served via **async lookup** (`lookup.async=true` default),
  indexed on the PK — Flink never materializes `users` in its own state.

## Why this is the headline complement pattern
- Kafka stays the **ingestion bus** (orders arrive as a normal topic).
- Fluss is the **dimension/state tier**: indexed, point-queryable, updatable in place
  (S6 partial updates), tierable to Iceberg (S4).
- Without Fluss you'd bolt on Redis/HBase for the lookup, OR hold the dimension in
  Flink keyed state and replay a compacted Kafka topic to rebuild it on every restart.
  The Fluss lookup removes both.
- Connector identifiers: source `'connector'='kafka'`, dimension lives in the
  `fluss` catalog; the temporal join is plain Flink SQL.

## Stability note (and the fix)
This run initially failed repeatedly on a single 6g TM that had been up ~16h:
`StaleMetadataException: Alive tablet server is empty`, TaskManager heartbeat timeouts,
and Kafka itself getting OOM-killed (exit 137). After the host Docker memory was raised
(~23 GB) and the TaskManager bumped to a 10g process (5.2 GB direct memory, up from the
332 MB default), plus a coordinated Fluss/Flink restart so the tablet server
re-registered, the job ran clean. Lesson for reproducers: give the TM real off-heap/direct
memory — Fluss's Arrow + Netty paths are direct-memory hungry, and an undersized TM
manifests as flaky registration and heartbeat timeouts, not an obvious OOM.
