# Internal streaming operators

The public programming model is a decorated Process started with `Client.start(...)`. Highwater derives versioned dependencies from awaited resources in the handler. Native Rust operators remain internal execution nodes used for standard incremental algorithms and engine tests.

The operator contracts below were derived from the Apache Flink checkout at `~/flink` (`4828c186`). We should reuse its state-machine invariants and test scenarios, not port its Java runtime classes. Every implementation remains a native Rust operator backed by this project's RocksDB and object-WAL transaction boundary.

## Ranked candidates

1. Window aggregation — tumbling and hopping forms implemented
   - Keyed count, sum, and max accumulators are updated per record and emit final windows after allowed lateness. Session assigners and merging state remain future extensions.
   - Extract from `WindowOperator`: pane namespaces, trigger state, merging-window metadata, cleanup at window end plus allowed lateness, and timer recovery.
2. Interval join — bounded inner join implemented
   - Joins two keyed streams when their event times fall within declared lower and upper bounds. Using the same stream on both sides produces strictly forward event-time pairs for sequence and co-visit analysis.
   - Extract from `TimeIntervalJoin`: independent left/right caches, per-side completeness, outer-join unmatched tracking, inclusive/exclusive bounds, and watermark-driven cleanup.
3. Deduplication — keep-first event-time form implemented
   - Keeps the first record per key according to event time after the completeness frontier passes it. Later records are durably classified as suppressed duplicates. Keep-last requires retractions and remains future work.
   - Extract from Flink's row-time deduplication operators: event-time ordering, equal-timestamp behavior, update-before/update-after output, late records, and retention.
4. Filter — field predicate form implemented
   - Evaluates a typed equality or numeric comparison for every accepted record and durably dispatches matching Process events without waiting for a watermark.
5. Keyed Process
   - Provides named keyed state, event-time timers, and side outputs as the foundational stateful escape hatch.
   - Extract from `KeyedProcessOperator`: timer namespaces, current-key isolation, recovery ordering, and deterministic timer callbacks. State schemas and handler upgrades must be explicit before this is exposed.
6. Async enrichment
   - Runs bounded-concurrency enrichment with ordered or unordered output, timeouts, and retries.
   - Extract from `AsyncWaitOperator`: checkpointed in-flight inputs, replay after recovery, capacity backpressure, timeout behavior, retry policy, and output ordering.
7. Broadcast state
   - Applies a low-volume rule or configuration stream to every keyed data partition.
   - Extract from `KeyedBroadcastProcessFunction`: deterministic broadcast updates, read-only access from the data side, state redistribution, and timer interaction.

## Intentionally excluded

- General map, flat-map, and union graphs remain outside the public API. Internal filters stay narrow because their matching Process and output are durable resources.
- Regular unbounded stream joins require retractions and potentially unlimited state; interval and temporal joins cover the practical bounded cases first.
- Processing-time temporal joins are nondeterministic under replay.
- SQL, catalogs, and a general dataflow builder are outside the intended programming model.
