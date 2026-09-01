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

Mailbox admission and source cursor advancement happen in the same transaction. Reaching capacity returns retryable HTTP 429; `StreamWriter` pauses without advancing its durable offset. Fenced invocation leases prevent concurrent handler commits. A failed transition leaves prior state unchanged and moves into a separately bounded retry lane, so it preserves ordering for its key without consuming normal-key concurrency. Batch failures are isolated to individual inputs. After `max_attempts`, the input is durably available from `/processes/{process_id}/quarantine`, its key is released, and processing continues.

## Distributed checkpoints

A coordinator checkpoint first creates a prepared local state handle at an exact WAL sequence and emits a barrier naming every active key-group owner and its epoch. Remote owners align their inputs at that cut, upload state, and acknowledge with the exact fenced epoch map. The coordinator publishes the manifest—and only then truncates the covered WAL—after all owners acknowledge. An ownership change fences a stale acknowledgement instead of mixing state from different assignments. State handles are retained in the manifest for object-store recovery.

## Current boundary

The remaining distribution work is remote shard execution of the barrier protocol, credit-based backpressure across edges, unaligned and incremental checkpoints, rescaling transfer, and a general multi-input operator scheduler.
