# S9 — Can you do lookup joins against Kafka? And what about checkpoints? (live, 2026-06-25)

Answers the question: does the S8c "stream + lookup into a dimension" pattern work with
Kafka as the dimension, and how do checkpoints compare?

## Test 1 — Processing-time lookup against Kafka: NOT SUPPORTED
A `FOR SYSTEM_TIME AS OF o.proctime` lookup with an `upsert-kafka` (PRIMARY KEY) table
as the dimension is rejected by the planner:

    org.apache.flink.table.api.TableException:
    Processing-time temporal join is not supported yet.

Why: a proctime lookup join requires the dimension connector to implement Flink's
`LookupTableSource` (point lookups by key). **JDBC, HBase, Hive, and Fluss do; the Kafka
connector does not** — it's a scan source. So you cannot point a proctime lookup at a
Kafka topic, even one with a declared PRIMARY KEY. (This is the clean, stateless join
that S8c does against Fluss.)

## Test 2 — Event-time versioned join against Kafka: WORKS, but stateful
The way youCAN use a Kafka PK table as a dimension is an **event-time versioned join**:
declare event-time + watermarks on both sides and join `FOR SYSTEM_TIME AS OF o.rowtime`.
This ran (job graph: `TemporalJoin` operator with TWO stateful sources). But:

| | Kafka versioned join (event-time) | Fluss lookup join (S8c, proctime) |
|---|---|---|
| Operator | **`TemporalJoin`** | **`LookupJoin`** (async) |
| Dimension lives in | **Flink keyed state** (materialized from the topic) | **Fluss** (indexed; not in Flink) |
| Checkpoint state | **78,317 bytes for a 2-ROW dimension** | dimension contributes ~nothing |
| Needs watermarks | yes (both sides) | no |
| Semantics | as-of event-time version | as-of processing-time |
| Restart cost | replay/rebuild dimension state from the topic | dimension already durable in Fluss |

The headline: the Kafka path **must hold the whole dimension in Flink state** (and grow
the checkpoint with it, and rebuild it on recovery by replaying the compacted topic),
because Kafka can't be looked up. **78 KB for two rows** — extrapolate to a real
dimension (millions of rows) and you're back to the same large-checkpoint /
slow-recovery problem delta join solves, just for the dimension side.

## The benefit of Fluss lookups, concretely
1. **Proctime lookups exist at all** — the simple "enrich each event against current
   dimension" join is only available because Fluss is a `LookupTableSource`. With Kafka
   you must use the heavier event-time versioned join (watermarks, dimension state).
2. **Dimension stays out of Flink state** → checkpoints stay tiny, recovery is fast, and
   you don't replay a compacted topic to rebuild the dimension on every restart.
3. **The dimension is also independently queryable / updatable** (PK point queries,
   partial updates S6) — a Kafka compacted topic is neither.

## Note
"Kafka as a Flink table with a primary key" is real (`upsert-kafka` + `PRIMARY KEY`),
and it's enough for an event-time versioned join — but it is NOT enough to be a
proctime lookup dimension, and it pushes the dimension into Flink checkpointed state.
That distinction is the whole reason Fluss's indexed PK tables matter for enrichment.

(Stability: the single-node stack intermittently loses tablet-server registration under
sustained load — `StaleMetadataException: Alive tablet server is empty` — and Kafka was
SIGKILLed (exit 137) twice during this session; a coordinated restart + a larger TM
(10g) recovers it. Numbers above are from clean runs.)
