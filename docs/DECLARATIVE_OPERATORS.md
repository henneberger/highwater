# Declarative operators

Operator specs are immutable deployment descriptions. `Client.deploy(spec)` validates a spec and reconciles it with a durable Rust operator carrying the same `operator_id`. Repeating an identical deployment is safe; changing an existing specification is rejected until state migration and upgrade semantics exist.

The operator contracts below were derived from the Apache Flink checkout at `~/flink` (`4828c186`). We should reuse its state-machine invariants and test scenarios, not port its Java runtime classes. Every implementation remains a native Rust operator backed by this project's RocksDB and object-WAL transaction boundary.

## Ranked candidates

1. `WindowAggregateSpec` — tumbling and hopping forms implemented
   - Keyed count, sum, and max accumulators are updated per record and emit final windows after allowed lateness. Session assigners and merging state remain future extensions.
   - Extract from `WindowOperator`: pane namespaces, trigger state, merging-window metadata, cleanup at window end plus allowed lateness, and timer recovery.
2. `IntervalJoinSpec` — bounded inner join implemented
   - Joins two keyed streams when their event times fall within declared lower and upper bounds. Using the same stream on both sides produces strictly forward event-time pairs for sequence and co-visit analysis.
   - Extract from `TimeIntervalJoin`: independent left/right caches, per-side completeness, outer-join unmatched tracking, inclusive/exclusive bounds, and watermark-driven cleanup.
3. `DeduplicateSpec` — keep-first event-time form implemented
   - Keeps the first record per key according to event time after the completeness frontier passes it. Later records are durably classified as suppressed duplicates. Keep-last requires retractions and remains future work.
   - Extract from Flink's row-time deduplication operators: event-time ordering, equal-timestamp behavior, update-before/update-after output, late records, and retention.
4. `FilterSpec` — field predicate form implemented
   - Evaluates a typed equality or numeric comparison for every accepted record and durably launches matching workflows without waiting for a watermark.
5. `KeyedProcessSpec`
   - Provides named keyed state, event-time timers, and side outputs as the foundational stateful escape hatch.
   - Extract from `KeyedProcessOperator`: timer namespaces, current-key isolation, recovery ordering, and deterministic timer callbacks. State schemas and handler upgrades must be explicit before this is exposed.
6. `AsyncEnrichmentSpec`
   - Runs bounded-concurrency activity-backed enrichment with ordered or unordered output, timeouts, and retries.
   - Extract from `AsyncWaitOperator`: checkpointed in-flight inputs, replay after recovery, capacity backpressure, timeout behavior, retry policy, and output ordering.
7. `BroadcastStateSpec`
   - Applies a low-volume rule or configuration stream to every keyed data partition.
   - Extract from `KeyedBroadcastProcessFunction`: deterministic broadcast updates, read-only access from the data side, state redistribution, and timer interaction.

## Intentionally excluded

- General map, flat-map, and union graphs remain outside the API. `FilterSpec` is intentionally narrow because its matching workflow and output are durable resources.
- Regular unbounded stream joins require retractions and potentially unlimited state; interval and temporal joins cover the practical bounded cases first.
- Processing-time temporal joins are nondeterministic under replay.
- SQL, catalogs, and a general dataflow builder are outside the intended programming model.
