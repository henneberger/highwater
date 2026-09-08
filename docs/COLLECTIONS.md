# Native changelog collections

Use native collections for changing query results such as leaderboards. They run
inside the server transaction and need no Python worker or workflow declaration.
Business Processes remain available for application logic and external effects.

```python
from highwater import Client, CollectionMode, StreamOptions, WatermarkMode
from highwater.dag import Dag

client = Client()
managed = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED, idle_timeout=None)
dag = Dag("leaderboard").stream("scores", managed).stream("leaders", managed)
dag.top_n(
    "top-scores", input="scores", n=10,
    partition_by=["league"], primary_key=["player_id"],
    order_by=[("score", "desc"), ("player_id", "asc")],
    input_mode=CollectionMode.UPSERT, output="leaders",
)
await dag.deploy(client)

await client.publish_event(
    "scores", {"league": "west", "player_id": "alice", "score": 100},
    event_time=10, kind="upsert",
)
# Replaces Alice's contribution, including movement to another league.
await client.publish_event(
    "scores", {"league": "east", "player_id": "alice", "score": 80},
    event_time=20, kind="upsert",
)
rows = await client.collection_rows("top-scores", group=["east"])
# Key-only deletion retracts the saved full row at its original timestamp.
await client.publish_event("scores", {"player_id": "alice"}, event_time=30, kind="delete")
```

`client.top_n(...)` accepts the same arguments without `output`; use
`client.connect_operator(id, output_stream)` to connect an existing operator to a
source-managed stream. `TopNSpec` and `NormalizeSpec` from `highwater.model` also support deployment
through the DAG. Inspect `dag.snapshot()` or `client.relational_operator(id)` for
the input mode, general retractable strategy, and retention/progress contract.
The server independently validates registration and known collection connections.

## Input contracts

Modes are declared on these operators. Existing stream and Process semantics
are unchanged; an undeclared legacy `upsert` still means an addition in existing
aggregates and views.

| Mode | Accepted input | State and deletion |
| --- | --- | --- |
| `append` | `insert` only | Retains candidates; no append-only optimization yet. |
| `retract` | `insert`, `update_after`, `update_before`, `delete` | Without primary keys, deletions match the full row and original event time and decrement multiplicity. With primary keys, keys must be unique and the saved full row is checked; its original timestamp is restored for deletion. |
| `upsert` | `insert`, `upsert`, `update_after`, `delete` | Requires primary keys. Additions replace the saved row; deletion may contain only the key. `update_before` is rejected because it belongs to a full retracting changelog. |

Primary keys are unique across the operator, independently of its grouping keys.
Use composite primary keys if a player ID is unique only within a tenant. For
retract mode, retract an old primary-key row before adding its replacement.
Unmatched deletes, duplicate primary-key insertions, invalid sort values, and
count overflow fail the transaction, including its source admission position.
Identical keyed upserts emit nothing and retain the original row time. Source
retry deduplication remains separate and uses event IDs/source offsets.

To normalize once before filters, windows, joins, or several other consumers:

```python
await client.normalize("score-rows", input="raw-scores", primary_key=["player_id"])
await client.connect_operator("score-rows", "normalized-scores")
```

Create `normalized-scores` as a source-managed stream first. The normalizer emits
full-row inserts/deletes, preserving the source key, so downstream top-N uses
`retract` mode. For a Process that emits rows containing a stable ID, use
`input_mode=CollectionMode.RETRACT` on the normalizer: its saved primary-key state
recovers the old row time from a before-image stamped with a later correction
time. Connect this normalized output to windows/interval joins. Direct legacy
Process-to-window/join connections retain their existing timestamp behavior.

A new collection's retracting output cannot feed keep-first deduplication, even
through intermediate native edges. The DAG catches known incompatible graphs
before deployment; the server also checks edge/consumer creation. This is scoped
capability validation, not a general optimizer for arbitrary legacy sources.

## Ordering, multiplicity, and reads

Top-N selects exactly N row occurrences per group, or all occurrences when fewer
exist. Duplicate bag rows count multiple times. It keeps all losing candidates,
so deleting a winner promotes the next available row. Changes contain individual
winner rows, not replacement lists. It suppresses unchanged winners and emits
unit insert/delete changes, compatible with existing edges. Edges explicitly
reject inconsistent or non-unit differences instead of silently losing weight.

Specify ascending/descending sort fields. Nulls sort last in either direction;
missing fields and array/object sort values are rejected. Each sort field has one
persistent non-null scalar type: number, string, or boolean. Integer sort values
must be within +/-2^53 to avoid lossy float comparisons. Dot-separated field paths
are supported. Add an explicit unique tie-breaker for meaningful equal-score
ordering; full serialized row identity and original event time provide the final
deterministic fallback. Rank columns, rank offsets, and all-ties selection are not
implemented.

Top-N output keys are compact JSON arrays of the grouping values: `["west"]`,
`["tenant-a","west"]`, or `[]` for global top-N. This keeps string, numeric, and
null groups distinct. `collection_rows(id, group=[...])` constructs this key.
Normalizer output keeps the incoming stream key; read it using `key="..."`.
Each returned row has `key`, `value`, `event_time`, and positive `count`. Reads are
unordered current rows; `order_by` selects membership, not HTTP response order.
Reads fail above `limit` (default/max 10,000), rather than silently truncating.
They scan indexed output, not changelog history, and create no saved snapshot.

`client.snapshot_views(id)` also supports a single native collection, reading its
maintained output under an `operator_output_cut`. It consolidates equal key/value
rows across timestamps. Collection snapshots allow at most 10,000 indexed rows;
existing multi-view snapshots still require same-source native filters. Saved
snapshots survive restart until deleted explicitly.

## Time, recovery, and scale

The row timestamp is preserved on retraction. A future deletion can promote a
very old losing candidate, so these unwindowed operators propagate no output
watermark until their input is sealed. Live membership updates and reads work
immediately; downstream completeness-gated work and final window triggers wait
for sealing. Use source-managed inputs when an inferred lateness cutoff would
drop valid corrections. Source admission still applies the configured late policy.

Registration backfills retained admitted records in global admission sequence in
the same control-shard transaction as activation. Source tail admission cannot
interleave with this transaction. Candidate rows, normalization state, current
outputs, and pending edge changes are journaled together and recovered on restart.
Before/after messages may be separately observable downstream; no cross-edge
atomic read or external-sink exactly-once guarantee is added.

Candidate storage grows with all live rows, not N. Ordered prefix reads fetch only
the first N candidate entries for each affected group; individual updates mutate
individual index entries, with O(N) membership comparison. Backfill still scans
retained input and can hold a large control transaction. Native execution remains
on the control shard; this release does not add distributed rank merging,
partition-owner execution, eviction/TTL, or a bounded-state approximation.

Run `PYTHONPATH=src python examples/changelog_leaderboard.py` against a local
server for an executable winner-promotion example.
