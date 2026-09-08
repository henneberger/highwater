# Changelog workflows: assessment and proposed operator model

Status: design review, 2026-09-08. The APIs below are proposals, not implemented
features. Highwater baseline: `4d4a3c1`. Flink source inspected in `~/flink` at
`4828c1869f815d2b9cef2e8617b47f5fad01edeb`. This is a survey of streaming
operator families and selected implementations, not a claim to have audited every
Flink algorithm or its batch, asynchronous, and generated-code variants.

Highwater has durable incremental execution, but it does not yet have a common
dynamic-table contract. Add that contract and reusable state primitives before
building a separate bespoke workflow implementation for every relational operator.
Top-N should be one declaration backed by a native ordered arrangement. A Python
Process should not need to implement candidate retention, retractions, rank
changes, output identity, or recovery to maintain a leaderboard.

## What exists, and where composition breaks down

| Layer | Current implementation | Consequence |
| --- | --- | --- |
| Row changes | `ChangeKind.weight()` maps insert, upsert, and update-after to +1, before/delete to -1 | `upsert(key, new)` does not replace an earlier row in aggregates or views. Two upserts mean two contributions. Version lookups have different semantics. |
| Transport | `DifferentialChange` includes `diff`; `StreamRecord` includes only `kind`; `append_internal_stream_change` copies the kind | Current producers use unit weights. Arbitrary consolidated weights would be lost across an edge; general weighted deltas are not an end-to-end capability. |
| Filters | Preserve kind and payload, and create workflow executions | Balanced before/after changes work across predicate boundaries. An after-only replacement cannot retract a formerly matching row without normalization state. |
| Windows | Count/sum/max emit changing aggregate rows; max keeps value counts | Useful incremental foundation, but count/sum do not validate full-row retraction identity. Max scans the value multiset on each update. There is no common aggregate interface. |
| Interval joins | Per-side arrangements, pair outputs, explicit pair deletion | Retraction matches key, value, and original event time. Missing matches are ignored. Pair removal scans the operator's outputs. This differs from the view's strict nonnegative result check. |
| Temporal joins | Buffered probes, version history and tombstones, completeness-driven output | An as-of lookup is a specialized temporal operation, not a continuously revisable arbitrary join. |
| Deduplication | Event-time keep-first, rejects negative kinds | A retracting upstream operator can deploy successfully and fail when the first negative row reaches this operator. |
| Python Processes | Arbitrary keyed transitions; `ctx.record` exposes kind; one retained emitted value per key | The user must interpret input retractions. Emitting a list replaces one list-valued row, not a collection of independently retractable rows. `emit=None` leaves prior output present; it is not deletion. |
| Edges | Durable pending queue, idempotent downstream admission, frontier propagation | Strong recovery foundation. Draining scans pending changes without a per-edge work budget. Native dispatch remains largely tied to control-shard transactions. |
| Views | Fold retained output changes when capturing a snapshot | Correct bag projection within documented boundaries, but reads grow with history. Process output snapshots and arbitrary multi-input consistent cuts are unsupported. |
| DAG | Deployment lowering and cycle/dependency checks | No inferred changelog modes, unique keys, retraction capabilities, or algorithm selection. Native specs still require a workflow even when only their maintained output is wanted. |

Source entry points: [row kinds and records](../crates/server/src/streaming.rs),
[edge transport and windows](../crates/server/src/stream_engine.rs),
[native operators](../crates/server/src/operators.rs),
[Process output](../crates/server/src/process.rs),
[typed handler preparation](../src/highwater/streaming.py),
[DAG lowering](../src/highwater/dag.py), and
[view folding](../crates/server/src/views.rs).

One concrete composition hazard deserves a regression test before expanding the
catalog: a Process emits row A at event time 10, then replaces it at time 20.
`finish_sharded_process_execution` stamps both `update_before(A)` and
`update_after(B)` with 20. An interval join that stored A at 10 searches for A at
20 when retracting it and does not find it. A window can similarly assign the
retraction to a different window. This follows from the current code paths; this
review has not added an integration reproduction. Define whether time is a row
attribute or a change timestamp before changing either operator in isolation.

## Lessons from the Flink operator families

Paths below are relative to the local Flink checkout. Runtime paths are under
`flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/`.
These describe reusable algorithmic patterns; they do not assert that every Flink
variant accepts arbitrary retractions.

| Family | Flink source entry points | Generalization for Highwater |
| --- | --- | --- |
| Changelog normalization and planning | Planner `FlinkChangelogModeInferenceProgram.scala`, `ChangelogNormalizeRequirementResolver.java`; runtime `deduplicate/utils/DeduplicateFunctionHelper.java` | Track accepted/produced kinds and primary keys. Recover old values before filtering, rekeying, joining, or aggregating after-only upserts. |
| Projection, filter, flat-map/correlate | `calc/`, `correlate/` | Deterministic transforms preserve weights. Multiple input rows that map to the same output require consolidated multiplicities. Nondeterministic transforms need persisted prior results to retract correctly. |
| Group aggregates and table aggregates | `aggregate/utils/GroupAggHelper.java`, `GroupTableAggFunction.java` | Accumulate/retract/value, optional merge, row count for empty groups, output equality suppression; table aggregates emit a changing multiset. |
| Mini-batch and local/global aggregates | `aggregate/MiniBatchGroupAggFunction.java`, `MiniBatchLocalGroupAggFunction.java`, `MiniBatchGlobalGroupAggFunction.java` | Combine updates by key and emit the net output difference; distributed partial aggregation requires an explicit merge law. |
| Distinct and deduplication | `deduplicate/` and aggregate data views | Distinct is count crossing zero. Keep-first/last is ordered selection with a replacement candidate when the winner disappears; append-only variants can use less state. |
| Top-N and rank | `rank/AppendOnlyTopNFunction.java`, `RetractableTopNFunction.java`, `UpdatableTopNFunction.java`, `TopNBuffer.java` | Ordered candidates plus multiplicity, explicit tie/rank semantics, and algorithm selection from proven input properties. |
| Window top-N | `rank/window/processors/SyncStateWindowRankProcessor.java` | Reuse ordered selection in a window namespace; final emission and cleanup follow the window's progress contract. |
| Regular inner/outer joins | `join/stream/StreamingJoinOperator.java` | Two indexed multisets. Delta matches multiply weights; outer joins also track match counts and add/remove null-extended rows at zero crossings. |
| Semi/anti joins | `join/stream/StreamingSemiAntiJoinOperator.java` | Match-count transitions determine whether a left row exists in the output. |
| Interval joins | `join/interval/RowTimeIntervalJoin.java` | Key/time range indexes and opposite-input progress determine safe retention. |
| Temporal joins and lookups | `join/temporal/TemporalRowTimeJoinOperator.java`, `join/lookup/` | Keep predecessor versions across cleanup, buffer probes until progress permits evaluation, distinguish event-time as-of from processing-time/external lookup. |
| Tumbling/sliding/session windows | `window/groupwindow/operator/WindowOperator.java`, `window/groupwindow/internal/MergingWindowProcessFunction.java` | Separate assignment, aggregate state, trigger policy, and cleanup. Session merges need to retract superseded outputs; arbitrary event deletion can also require session splitting. |
| OVER aggregates | `over/ProcTimeRowsBoundedPrecedingFunction.java` and row-time variants | Ordered frame buffers, entering/leaving contributions, timers. Sliding-frame retraction support does not itself prove support for arbitrary input changelogs. |
| Streaming sort | `sort/StreamSortOperator.java`, `RowTimeSortOperator.java` | Complete global sort needs bounded input; temporal ordering needs progress. A live top-N is a different contract. |
| Pattern recognition | `match/`, CEP library | Automata, event history, match selection, and timers. Retracting an event can revise many matches; share storage/progress but retain a specialized kernel. |
| User table functions, async work, ML/search | `process/`, `AsyncStateTableStreamOperator.java`, `ml/`, `search/` | Explicit input/output capabilities and durable result identity. Arbitrary external effects are not algebraically reversible. |
| Sinks | `sink/SinkUpsertMaterializer.java` and newer variants | Convert multiset changes into a declared keyed sink contract; handle changelog order, stable message identity, and destination deduplication separately. |
| Scheduling and watermarks | `multipleinput/`, `wmassigners/`, `bundle/` | Input selection, bounded work, batching, and progress accounting belong in shared execution machinery, not each operator. |

Union-all, distinct union, intersection, and difference fit the same weighted
collection model: union-all adds weights; distinct thresholds positive counts;
bag intersection uses the minimum count; bag difference uses the positive part
of the count difference. These are proposed algebraic extensions, not claims
about individual Flink runtime class implementations inspected here.

The most consequential Flink design is the planner/runtime split.
[RankProcessStrategy](../../flink/flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/utils/RankProcessStrategy.java)
selects append, retract, or monotonic-update strategies using input metadata.
[RetractableTopNFunction](../../flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/rank/RetractableTopNFunction.java)
retains sort-key row lists and sorted counts, including candidates outside N.
[UpdatableTopNFunction](../../flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/rank/UpdatableTopNFunction.java)
requires unique keys containing the partition key, improving sort values, and no
delete/before messages. Its small-state optimization is not valid for arbitrary
score changes. Some Flink kernels tolerate missing state or warn about TTL-driven
incorrectness; Highwater should define its own strictness policy explicitly.

Flink's public [dynamic-table explanation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/concepts/dynamic_tables/)
also distinguishes append, retract, and keyed upsert encodings. That distinction
should be visible in Highwater's declarations even without adding SQL.

## Proposed shared contract

1. **Collections and events are distinct inputs.** Declare append, keyed-upsert,
   or retracting-multiset mode. Keep existing streams in their legacy mode;
   changing the meaning of existing `UPSERT` data would alter replay results.
   A normalizer for keyed upserts stores the prior row and emits `-old, +new`.
   Key-only deletes must pass through it. Partition/group key and row primary key
   are separate concepts. Normalization happens before repartitioning so a group
   key change retracts from the old group and inserts into the new group.
2. **One weighted representation inside relational execution.** Use
   `(group_key, row, diff, change_time, source_position)` with checked integer
   multiplicity, stable row equality, and explicit schema/number/null rules.
   Row kinds remain an external encoding. Persist weights through source records,
   edges, replay, and output delivery; until that migration exists, reject
   non-unit weights instead of silently dropping them. Bag inputs must provide
   the full retracted row; a declared unique-key adapter may resolve it.
3. **Separate row time from change time.** The time used for window assignment or
   interval matching is part of the logical row and survives its retraction.
   The later time at which a correction is processed is separate. Late-data
   admission and reclamation must say which time they use. A correction to a
   finalized time is rejected or routed under an explicit policy, not silently
   applied to another window.
4. **Common state primitives.** A counted row arrangement; an ordered multiset
   with range/first-N access; a versioned predecessor index; aggregate state with
   accumulate/retract and optional merge; a maintained output multiset; and
   durable timers. Implement indexes with RocksDB prefix/range access and
   transaction-overlay visibility. Avoid full candidate/history scans in each
   transition. Store identity separately from transport metadata.
5. **One transition boundary.** Consume a durable input position, mutate keyed
   state, compare previous/new outputs, and enqueue net changes atomically under
   the owner epoch. Batch changes from one logical replacement should have a
   durable boundary if downstream readers require atomic visibility. State/WAL
   atomicity alone does not give a multi-edge reader a consistent cut.
6. **Capability-aware deployment.** Each operator declares accepted modes, output
   mode, unique/group keys, ordering requirements, monotonicity, time policy,
   state version, and optional mergeability. `Dag` validates the whole graph
   before creating resources, inserts explicit normalizers/exchanges, and
   reports the selected algorithm and retention in its inspectable plan.
   Server validation must enforce the same contract for non-Python clients.
7. **Progress and retention are operator-specific.** A scalar event-time frontier
   is sufficient for the current acyclic scope; it is not Differential Dataflow's
   partially ordered logical-time runtime. Include buffered inputs, pending
   outputs, retries, and async work in output progress. Exact unwindowed top-N
   cannot forget candidates simply because wall-clock TTL expires. Apply
   backpressure or fail at a stated capacity limit; semantic expiration must
   emit retractions. Physical reclamation still requires checkpoint coverage.
8. **Effects are explicit consumers.** Native relational operators should not
   require creating a workflow per change. Provide a separate workflow/sink
   subscription with stable delivery IDs. Existing workflow declarations remain
   compatible. Python Processes retain command/event semantics by default;
   opt-in relational handlers need explicit retractable output collections,
   including deletion, rather than an overloaded `emit=None`.

Materialized reads then use the same output arrangement, updated in the operator
transaction. Capture should pin a supported cut or copy indexed rows, not replay
all retained history on every keyed read. Recovery, backfill, deployment changes,
and replay must use the same normalizers and kernels. An index bootstrap needs a
captured input position plus tail catch-up before activation. Comparator, schema,
or changelog-mode changes require a rebuild or explicit migration.

## Top-N as the first complete implementation

Proposed Python declaration, deliberately using native fields rather than Python
callbacks that would execute outside the native transaction:

```python
dag.top_n(
    "leaders",
    input="score_changes",
    partition_by=["league"],
    primary_key=["player_id"],
    order_by=[("score", "desc"), ("player_id", "asc")],
    n=10,
    output="leader_changes",
)
```

The input declaration supplies its changelog mode. Primary keys are unique within
the declared scope; the example groups by league and breaks score ties by player
ID. Start with deterministic row-number membership, a fixed positive N, declared
scalar sort types, and explicit null ordering. Treat `RANK`, `DENSE_RANK`, offsets,
and all-ties output as later features: ties can make the result larger than N.
Default output is one row per winner, with no rank field. Opting into rank numbers
requires updates for shifted ranks even when membership is unchanged.

For each group, retain all live candidates in a counted ordered index and the
published winner multiset. Normalize input, capture the old first N, apply the
delta, compute the new first N, and emit the multiset difference. Suppress equal
outputs. Reject unmatched retractions consistently. Updates that change grouping
or ordering remove the old entry before adding the new one. An append-only
strategy may retain just the best N; select it only when the graph proves that
negative or replacing updates cannot arrive, and enforce that at admission.

For N=2, descending score:

| Input | Winners after input | Output changes |
| --- | --- | --- |
| Insert A=100 | A | +A |
| Insert B=90 | A, B | +B |
| Insert C=80 | A, B | none |
| Delete A=100 | B, C | -A, +C |
| Replace B=90 with B=70 | C, B=70 | -B=90, +B=70 |

C must remain in state even while invisible. General top-N uses O(M) candidate
storage for M live rows in a group. With an ordered index, aim for O(log M) index
mutation plus O(N) winner comparison/output work; do not claim that cost for a
whole-map serialized state implementation. Rank-number output can inherently
change O(N) rows. Measure bytes, seek counts, and write amplification too.

Partitioned leaderboards distribute by group. A single global leaderboard is one
logical group unless a later two-stage plan is implemented. Local top-N followed
by global top-N can work with stable identity/order, but local stages still need
all losing candidates for arbitrary retractions and the merge needs a consistent
progress contract. Do not claim general global scalability from hash partitioning.

## Delivery order and acceptance criteria

1. Add composition regressions and mode metadata: after-only upserts across a
   filter, changing-time Process output into a window/join, negative input into
   keep-first dedup, and edge weight preservation or rejection. Preserve legacy
   deployments while making new relational declarations explicit.
2. Build normalization, row identity, time separation, and common counted/ordered
   arrangements. Add net-output emission and incremental keyed view reads.
   Reuse the existing WAL/outbox and owner fencing.
3. Deliver top-N through Rust validation/kernel/storage, public registration and
   reads, Python spec and DAG lowering, edges, snapshots, restart, and examples.
   It must run without a Python worker or dummy workflow. Ship the general
   retracting strategy first; append/monotonic optimizations come after proofs.
4. Move count/sum/max and distinct onto the common primitives, then regular and
   interval joins, temporal lookup, window namespaces and advanced ranking.
   Keep CEP, async external work, and business Processes as specialized kernels
   with explicit capabilities. Add bounded edge scheduling and partition-owner
   execution before promoting this as a scalable general streaming engine.

For every relational kernel, compare the incrementally maintained result against
a simple full recomputation oracle on generated valid update histories. Check
duplicate multiplicity, equal scores, removal of winners and non-winners, empty
groups, primary/group-key changes, all retraction forms, null/type validation,
and overflow. Compare quiescent logical update boundaries, not necessarily the
transient half of a before/after replacement. Also test rejected invalid histories.

Integration acceptance requires normalized upsert -> filter -> top-N -> aggregate
-> view composition; restart during pending output; duplicate input retries;
ownership handoff; backfill plus concurrent tail; and watermark/late-data behavior.
Run the opt-in S3 chaos suite for the new state and outputs before making stronger
distributed guarantees. Benchmark large losing-candidate sets and hot groups to
ensure work is indexed rather than proportional to retained changelog history.

This review adds no new runtime guarantees. The existing baseline passed Rust
workspace tests (43), Python discovery (95 run, four opt-in S3 tests skipped), Rust
format checks, a server build, and the documentation website build. That verifies
the baseline changes, not the proposed generalized model or the composition
hazards identified above.
