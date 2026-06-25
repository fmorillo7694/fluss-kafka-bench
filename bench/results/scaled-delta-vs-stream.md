# Scaled result: delta join vs stream-stream join at 200k keys (live, 2026-06-24)

The toy run (`headline-delta-vs-stream.md`) proved the *mechanism*. This proves the
*payoff* at real key cardinality.

## Setup
- Stack: Flink 2.2.1, Fluss 0.9.1-incubating, single TaskManager (6g, 4 slots,
  RocksDB state backend, checkpoint interval 10s).
- Data: `users_pk` = **200,000** distinct users, `orders_pk` = **200,000** orders,
  both Fluss PK tables with `table.merge-engine=first_row`, 8 buckets, joined on
  `user_id` (the bucket key / PK prefix).
- Same query both times: `INSERT INTO <sink> SELECT ... orders INNER JOIN users`.
  Only difference: `table.optimizer.delta-join.strategy` = AUTO vs NONE.

## Result

| Metric | Delta join (AUTO) | Stream-stream join (NONE) |
|---|---|---|
| Join operator in the graph | **`DeltaJoin`** | stateful **`Join`** |
| Completed checkpoints | 4+ | 9+ |
| Failed checkpoints | 0 | 0 |
| **Checkpoint state size** | **~54 KB** (40,637–68,870 B) | **~14.2 MB** (14,174,989 B) |

**≈260x smaller checkpoint state for delta join** at 200k keys.

## Why
The stream-stream join keeps BOTH 200k-row input sides materialized in Flink keyed
state (RocksDB), and that state is written into every checkpoint. The delta join
keeps almost no operator state — on each input row it does a prefix-key lookup
against the OTHER table's index *inside Fluss*. So the join "state" is the Fluss PK
table (durable, tiered independently of Flink), and Flink checkpoints stay tiny.

The gap widens with key cardinality: at 6 rows it was 34KB vs 42KB (~1.2x); at 200k
rows it is 54KB vs 14.2MB (~260x). Extrapolate to millions of keys and the
stream-stream side is where the "multi-GB checkpoints, slow recovery" pain comes
from — exactly what delta join is designed to remove.

## Honest caveats
- Single laptop TaskManager; absolute numbers are not production figures, the RATIO
  is the point.
- Delta join trades Flink state for **read load on Fluss** (every probe is a lookup).
  That cost lands on the TabletServers, not in this checkpoint number — a fair
  comparison must also watch Fluss CPU/IO (next scenario).
- Delta join is INNER-only and insert-only-source here; an outer join or a CDC source
  changes the calculus.
