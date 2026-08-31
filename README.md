# temporal-code

A compact Temporal-like service with a Rust execution core, annotation-discovered Python workflows, deterministic replay, hot local RocksDB state, and durable recovery through object-store changelogs.

## Architecture

```text
Remote clients ──HTTP──► Rust execution core
                            ├── RocksDB: hot state, histories, queues, timers
                            ├── Object storage: sharded WAL + vector checkpoints
                            ├── Fenced key groups, source cursors, transactional outbox
                            ├── Event-time streams, watermarks, native window state
                            └── Versioned activations ──► Python workers
                                                        ├── deterministic replay
                                                        └── activities
```

The Rust core is the authority for histories, timers, retries, task queues, and leases. Keyed processes hash to independent WAL lanes; each lane group-commits concurrent ingress and completion batches before RocksDB is updated as a disposable local cache. Atomically published RocksDB checkpoints record a position for every lane, bounding recovery to their WAL tails. Python workers execute leased activations and never own durable scheduler state.

## How to think about it

The primary abstraction is a **durable process**, not a stream topology. A workflow is one durable process with an explicit identity and arbitrary long-running control flow. A keyed process is a large family of smaller durable processes—one per business key—fed by events. Both execute ordinary Python through durable leases; workflows replay history, while keyed processes receive compact state-plus-event activations. Rust owns their durable state, ordering, admission, and recovery.

For a keyed process, each event causes one isolated state transition:

```text
event ──► durable mailbox ──► one workflow handler ──► new private state + optional output
             per key             leased/retried          committed atomically
```

Only one handler can run for a key, so application state needs no locks. Different keys run concurrently up to the deployment limit. When the bounded mailbox is full, producers receive retryable backpressure before their event or source offset is acknowledged. A crash can therefore repeat a leased workflow activation, but cannot lose an admitted event, run two committed transitions for one key, or partially publish state.

Most application code should not reason about partitions, checkpoint barriers, or watermark arithmetic:

```python
@process.defn(
    key="account_id",
    event_time="occurred_at",
    wait_until=process.complete,
)
@dataclass
class AccountBalance:
    balance: int = 0

    @process.event
    async def apply(self, event: Deposit):
        self.balance += event.amount
        return {"balance": self.balance}

accounts = await client.start(AccountBalance)
await accounts.send(Deposit("account-a", 5, occurred_at))
await accounts.finish()
balance = await accounts.state("account-a")
```

The dataclass fields are durable per-key state. Mutating them defines the next state, the handler return value is an optional changelog output, and `None` emits nothing. `start()` creates a private direct-input stream; passing `source="events"` attaches an existing stream. URI connectors, including Kafka, are intentionally not implemented. `ProcessSpec`, `ProcessContext`, and `process.transition(...)` remain the explicit expert API.

Input, process leasing, and state/output completion use continuous microbatches internally. Concurrent producers are group-committed per WAL lane. An execution batch is released when it reaches its size limit or when its oldest event reaches its delay limit. Ordinary `@process.event` handlers still receive one event at a time. Expensive vectorized work can opt into one application-visible call:

```python
@process.defn(key="document_id")
class Embeddings:
    @process.batch(max_size=128, max_delay=0.025)
    async def embed(self, documents: list[Document]):
        vectors = model.encode([document.text for document in documents])
        return [
            {"document_id": document.document_id, "embedding": vector}
            for document, vector in zip(documents, vectors, strict=True)
        ]
```

Under load this calls the model with up to 128 documents. With one document it flushes after 25 ms rather than waiting for a full batch. The runtime requires exactly one result per input and converts each result into its own durable state/output transition. A batch failure commits none of its completions, so every leased event is retried.

### Throughput

The included durable end-to-end benchmark completed 100,000 distinct keyed transitions at **108,164 events/s**, with **136,190 events/s** ingress, using ten data WAL lanes, ten Python worker processes, 10,000-event producer batches, and 12,000-event server activation batches. Every accepted input and completed state transition was synced to the object WAL before acknowledgement; this was not an in-memory or watermark-only measurement.

```bash
PYTHONPATH=src:. python benchmarks/process_throughput.py \
  --events 100000 --publishers 64 --batch-size 10000 \
  --activation-batch-size 12000 --max-concurrency 100000
```

This single-host result is a reproducible capacity measurement, not an availability claim or SLA. Small producer calls remain latency-oriented and have lower throughput; concurrent calls are group-committed for 500 microseconds rather than waiting for a full batch. See [`docs/DURABILITY.md`](docs/DURABILITY.md) for the exact failure boundary and [`docs/SCALING.md`](docs/SCALING.md) for the architecture beyond one host.

`IMMEDIATE` handles an event as soon as its key and a concurrency permit are available. `COMPLETE` is the event-time gate: it handles an event only after the input is known complete through that timestamp. `complete_through(t)` is a source assertion with durable consequences—later events at or before `t` follow the configured late-data policy. Connectors may make the same progress assertion continuously; bounded inputs may seal and release everything.

### Safe process upgrades

Every process deployment has an immutable build ID derived from its handler and migration code. Workflow tasks are pinned to that build, so a new worker cannot replay in-flight work created for old code. The server activates a new build only when the process identity, input, keying, and event-time contract are unchanged; concurrency, capacity, and task queue remain operational settings.

Persisted state is always an envelope containing its schema version and producing build. Schema versions only move forward, and every intervening migration must be declared explicitly:

```python
@process.defn(key="account_id", state_version=2)
@dataclass
class AccountBalance:
    balance: int = 0
    currency: str = "USD"

    @process.migrate(from_version=1)
    def migrate_v1(self, state):
        return {**state, "currency": "USD"}

    @process.event
    async def apply(self, event):
        self.balance += event.amount
```

Migrations run lazily inside the first transition handled by the new build and commit atomically with that event. During a rolling upgrade, keep old workers alive until their pinned tasks drain, start workers containing the new build, then call `start()` or `deploy_process()` with the new definition. This project has no released legacy state or wire format, so missing versions, raw state values, and unversioned transition results are rejected rather than guessed.

The underlying streams remain an expert integration surface for connectors, native joins/windows, changelog composition, and diagnostics. Ordinary process code only names its input; `ProcessSpec`, `StreamWriter`, explicit watermarks, operator edges, and checkpoint APIs are available when deeper control matters.

The protocol is language-neutral and versioned in [`proto/activation.proto`](proto/activation.proto). Its payload envelope supports JSON, Protobuf, raw bytes, and Arrow IPC streams. JSON is the implemented Python codec; Arrow is intentionally reserved for large columnar values and will be stored by reference rather than embedded in workflow history.

Rust is the only server implementation. Python is an SDK and language worker: it discovers annotations, deterministically replays workflow code, executes activities, and communicates commands back to Rust. The core implements workflow start/status/history, signals, queries, updates, cancellation, termination, activities with retries and heartbeat leases, timers, child workflows, version markers, and continue-as-new.

The current research implementation uses a filesystem directory as object storage. Key-group and source-partition leases use monotonically increasing epochs for fencing, while the current binary remains the single control-plane process. The exact implemented recovery contract and its current failure boundary are documented in [`docs/DURABILITY.md`](docs/DURABILITY.md). A conditional-write remote object-store adapter and replicated shard log remain required before treating a single server process as highly available.

## Event-time streaming

Streams are durable partitioned logs. Every record carries event time separately from ingestion time. Each partition produces a monotonic watermark using bounded out-of-orderness or an explicit source watermark; the stream watermark is the minimum watermark of all active partitions. Idle partitions stop holding back progress, sealed partitions model bounded input, and the combined watermark never moves backwards.

Allowed lateness creates two useful regions. Records at or behind the watermark are late but remain eligible until `watermark - allowed_lateness`; records behind that boundary follow the configured `drop`, `side_output`, or `accept` policy. `accept` is for raw-log consumers and cannot be combined with final window schedules because those deliberately do not produce correction panes or retractions. Watermark alignment rejects a partition that advances farther than the configured drift from slower active partitions, providing source backpressure instead of unbounded downstream buffering.

Fixed-window schedules incrementally maintain keyed `count` or `sum` accumulators in RocksDB and launch finite workflows after `window_end + allowed_lateness` passes the watermark. Workflow inputs contain the compact aggregate rather than rescanning and embedding the raw window. Workflows can also durably suspend on `wait_for_watermark(stream, event_time)`, the event-time counterpart to a wall-clock timer. Sealing every partition finalizes the stream, fires outstanding event-time timers, and closes its remaining non-empty windows. The design follows [Flink watermark generation, idleness, and alignment](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/) and [Beam watermark, trigger, and allowed-lateness semantics](https://beam.apache.org/documentation/programming-guide/).

For replayable sources, `publish_event` accepts a source ID, partition, and monotonically contiguous offset. The record and next source cursor commit atomically, so a restart cannot acknowledge input without remembering its position. Completed workflow results enter the `workflows` transactional outbox in the same state transition. Sink consumers lease messages and acknowledge them after applying the deterministic message ID; an idempotent sink therefore extends exactly-once effects across the service boundary.

Casual producers should use `Client.stream_writer(...)`. It resumes the source cursor, claims and renews a fenced source epoch, converts timezone-aware datetimes, and treats watermark alignment as backpressure. Streams default to bounded watermarks with five seconds of out-of-orderness and sixty-second idleness; monotonic and source-managed modes are explicit. `stream_info()` identifies watermark-blocking partitions and the current completeness frontier.

Operators use Flink-style changelog row kinds plus signed differences. Count, sum, max, and interval joins apply inserts and retractions incrementally. `connect_operator` durably feeds those changes into another source-managed stream exactly once, including watermark progress. Multi-owner checkpoint barriers publish an object-store manifest only after every fenced key-group owner acknowledges its state handle. The precise contract and current distributed-dataflow boundary are documented in [`docs/STREAMING_MODEL.md`](docs/STREAMING_MODEL.md).

`ProcessSpec` hides most streaming machinery behind a Temporal-style keyed process. Each key has private durable state and a serialized mailbox, while configurable concurrency permits different keys to run in parallel. Bounded capacity backpressures producers without losing source offsets. `EventTimeGate.COMPLETE` means “run only when this event timestamp is complete”; the runtime handles the watermark frontier and releases bounded input when it is sealed.

Event-time temporal joins are asymmetric as-of joins between a probe stream and a primary-keyed version stream. A probe at time `T` is released as soon as both streams are complete through `T`; the operator selects the latest version whose event time is at or before `T`, then launches a durable workflow for additional predicates. There are no join windows, and version records may be arbitrarily older than probes. Upserts, tombstones, left and inner joins, late-data safety, durable probe buffers, watermark-driven version cleanup, and idempotent event IDs are supported.

Applications declare operators with `WindowAggregateSpec`, `FilterSpec`, `TemporalJoinSpec`, `IntervalJoinSpec`, or `DeduplicateSpec` and deploy them idempotently through `Client.deploy(spec)`. Windows support tumbling or hopping assignment and native count, sum, and max state. Interval joins may join two streams or produce ordered forward pairs from one stream. REST resources remain internal control-plane targets. Additional spec candidates derived from the local Flink implementation are ranked in [`docs/DECLARATIVE_OPERATORS.md`](docs/DECLARATIVE_OPERATORS.md).

## Install

```bash
cd ~/temporal-code
python3.13 -m venv .venv
. .venv/bin/activate
pip install -e .
```

## Start and submit

Build and start the execution core:

```bash
cargo build --release -p temporal-code-server
./target/release/temporal-code-server \
  --state-dir example-rust-state \
  --object-store-dir example-rust-objects \
  --listen 127.0.0.1:7233 \
  --node-id local --key-groups 128 --log-shards 9
```

The RocksDB crate generates native bindings. On macOS, if the linker cannot locate `libclang`, expose the Xcode or Command Line Tools library directory before the first build:

```bash
export LIBCLANG_PATH="$(dirname "$(xcrun --find clang)")/../lib"
export DYLD_FALLBACK_LIBRARY_PATH="$LIBCLANG_PATH"
```

Start Python workers in separate processes. A worker may poll every queue, or use `--task-queue orders` to bind it to one queue:

```bash
PYTHONPATH=src:. python -m temporal_code.rust_worker examples.catalog \
  --target http://127.0.0.1:7233
```

Submit the examples from a third process:

```bash
PYTHONPATH=src:. python examples/submit.py --target http://127.0.0.1:7233
```

Run the event-time example against the same server and worker:

```bash
PYTHONPATH=src:. python examples/event_time_windows.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/temporal_join.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/interval_join.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/deduplicate.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/iot_sensor_metrics.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/clickstream_recommendation.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/durable_process.py \
  --target http://127.0.0.1:7233

PYTHONPATH=src:. python examples/batched_embeddings.py \
  --target http://127.0.0.1:7233
```

## Workflow surface

- `@workflow.defn`, `@workflow.run`, `@workflow.signal`, `@workflow.query`, `@workflow.update`
- `@activity.defn`, retry policies, non-retryable failures, timeouts, and heartbeats
- durable `execute_activity`, `sleep`, `wait_for_watermark`, `wait_condition`, `execute_child_workflow`, and `continue_as_new`
- workflow IDs, remote handles, results, cancellation, termination, and history
- leased workflow/activity task queues, parent-close policies, and version markers
- declarative tumbling/hopping aggregation, streaming filters, temporal joins, retractable bounded/self interval joins, and event-time keep-first deduplication
- durable operator edges and fenced distributed checkpoint-barrier coordination
- keyed durable processes with serialized state, bounded concurrency, mailbox backpressure, and event-time gates
- partitioned event-time logs, bounded out-of-orderness, idleness, alignment, late side outputs, and finite inputs

Payloads and results must be JSON serializable. Workflow code must be deterministic: use activities for I/O and use the workflow `now()` and `get_version()` APIs. Update handlers are synchronous.

## Examples

| Area | Example |
| --- | --- |
| Decorators, signals, queries, updates, conditions | [`examples/order.py`](examples/order.py) |
| Activity retries, timeouts, heartbeats, non-retryable errors, compensation | [`examples/reliable_activities.py`](examples/reliable_activities.py) |
| Timers, children, parent-close policy, deterministic time/info, versioning, continue-as-new | [`examples/children_and_versioning.py`](examples/children_and_versioning.py) |
| Task queues, leases, workflow timeout options, cancellation, termination, event history | [`examples/lifecycle.py`](examples/lifecycle.py) |
| Checkpointing and rebuilding local RocksDB from object storage after restart | [`examples/recovery.py`](examples/recovery.py) |
| Event time, source cursors, key-group epochs, incremental window sums, late data, and finite schedules | [`examples/event_time_windows.py`](examples/event_time_windows.py) |
| Bounded event-time interval join with immediate pair emission | [`examples/interval_join.py`](examples/interval_join.py) |
| Versioned-table temporal join, tombstones, conditions, incremental watermark release, and retry deduplication | [`examples/temporal_join.py`](examples/temporal_join.py) |
| Event-time keep-first deduplication with out-of-order input | [`examples/deduplicate.py`](examples/deduplicate.py) |
| Immediate sensor alerts and keyed hopping-window maximums | [`examples/iot_sensor_metrics.py`](examples/iot_sensor_metrics.py) |
| Ordered clickstream co-visits and event-time content enrichment | [`examples/clickstream_recommendation.py`](examples/clickstream_recommendation.py) |
| Temporal-style keyed isolation, durable state, backpressure, and an event-time completeness gate | [`examples/durable_process.py`](examples/durable_process.py) |
| Size-or-latency batched embedding inference with per-document durable outputs | [`examples/batched_embeddings.py`](examples/batched_embeddings.py) |

Deferred execution-core work is tracked in [`docs/FUTURE_WORK.md`](docs/FUTURE_WORK.md).

## Test

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```
