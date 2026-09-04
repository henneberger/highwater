# Streaming model

The execution core follows Flink's incremental state and changelog model without copying its SQL or DataStream APIs. Records update keyed RocksDB state one transition at a time. Operators publish durable row changes using `insert`, `update_before`, `update_after`, and `delete`, each with a signed `diff`. A materialized row is the sum of those changes, which also gives us the weighted-delta foundation associated with differential dataflow.

This distinction is intentional: Flink is a stateful streaming dataflow engine whose dynamic tables produce changelog streams. Differential Dataflow is a specific weighted, partially ordered computation model. This project adopts signed differences and incremental arrangements where useful, but does not claim to run the Differential Dataflow runtime.

## Event-time sources

`StreamWriter` is the normal publishing API. It resumes the durable source cursor, claims a fenced epoch for one stream partition, retries watermark-alignment backpressure, and commits each source offset atomically with its record. A bounded writer may seal its partition when its context exits.

Streams support three watermark modes:

- `bounded` derives a watermark from the greatest observed event time minus configured out-of-orderness. This is the default, with five seconds of disorder and a sixty-second idle timeout.
- `monotonic` uses each greatest observed event time directly.
- `source_managed` advances only when the source reports a watermark through its writer.

The stream watermark is the monotonic minimum across active partitions. Idle partitions are excluded, sealed partitions cannot reopen, allowed lateness determines the completeness frontier, and alignment applies backpressure to sources that outrun their peers. Stream inspection reports the blocking partitions and reason progress is stalled.

## Incremental operators

Window count, sum, and max are maintained incrementally. Insert-like records add weight; `update_before` and `delete` retract weight. Max windows retain a value multiset so retracting the current maximum reveals the correct previous maximum. Each state transition is available from `/operators/{operator_id}/changes` as a durable differential changelog.

Filters preserve the input row kind and signed difference. Interval joins maintain keyed arrangements on both inputs and retract every materialized pair when an input receives `update_before` or `delete`. Temporal joins buffer and can retract probes before their completeness frontier; after that frontier their as-of result is final because accepted-too-late input is disallowed. Version tombstones participate in version lookup. Keep-first deduplication remains append-oriented.

`connect_operator(operator_id, output_stream)` creates a native edge to a source-managed stream. Operator changes enter a durable pending queue in the same RocksDB/WAL transaction as operator state. The edge consumer atomically writes the downstream record and removes that pending item, so restart can replay without loss or duplication. It forwards row kinds unchanged and advances the downstream watermark only after earlier changes are visible. This is the composition path for incremental operators and Process consumers.

## Durable keyed processes

`@streaming.process` and `@streaming.event` are the application-facing streaming API. The typed handler receives the event plus `ProcessContext`, and returns `streaming.transition(state=..., emit=...)`. State remains private and durable; the optional emitted value produces `insert` followed by `update_before`/`update_after` changes, so a Process can feed another Process or native operator. `ProcessHandle` provides `send`, `state`, `complete_through`, and `drain`. `ProcessSpec` and the raw context envelope remain compatibility and expert APIs.

`EventTimeGate.IMMEDIATE` dispatches as soon as capacity permits. `EventTimeGate.COMPLETE` dispatches only after the input completeness frontier passes the event timestamp, providing an understandable “run when this event time is final” gate without exposing watermark arithmetic. Sealing bounded input releases all remaining events. Source-managed connectors should continue emitting watermarks even while data admission is backpressured, because progress messages are intentionally not subject to mailbox capacity.

Mailbox admission and source cursor advancement happen in the same transaction. Direct-ingress and stream-fed execution are mutually exclusive modes so one process key is never mutated through two journal shards. Direct ingress requires a stable event ID scoped to the process key and rejects reuse with different contents. Reaching capacity returns retryable HTTP 429; `StreamWriter` pauses without advancing its durable offset. Fenced invocation leases prevent concurrent handler commits. Returned failures and lost activation leases consume the same durable attempt budget and move into a separately bounded retry lane, preserving ordering for the key without consuming normal-key concurrency. Batch failures are isolated to individual inputs. After `max_attempts`, the event has a queryable terminal `FAILED` outcome, is durably available from `/processes/{process_id}/quarantine`, releases its key, and processing continues. Success atomically records state and a terminal `COMMITTED` outcome.

## Distributed checkpoints

Every service node can reconstruct every shard from the authoritative object journal. A checkpointing node first synchronizes each journal tail, then holds its local shard sequence guards while RocksDB creates a snapshot. The resulting manifest records the exact vector of covered journal positions. Records committed concurrently after synchronization are outside that vector and replay as journal tail during recovery.

The checkpoint files are uploaded before the current-checkpoint pointer advances conditionally. Only after publication may covered local WAL files be removed. Partition ownership does not require owner-local checkpoint acknowledgements because recovery uses the published full-state snapshot plus the authoritative per-shard journal tails.

## Current boundary

Remote checkpoints store files by SHA-256 digest. A new checkpoint uploads changed content, then conditionally advances its pointer. Restore verifies every file before opening the local RocksDB cache.

The remaining distribution work is credit-based backpressure across edges, unaligned checkpoints, rescaling transfer, and a general multi-input operator scheduler.
