# Highwater

Durable execution for streaming applications.

Highwater lets you write stateful stream processing as ordinary Python. It keeps each key ordered, persists every accepted event and state transition, tracks event-time progress, retries failed invocations, and scales execution with load.

```python
from dataclasses import dataclass
from highwater import process

@dataclass(frozen=True)
class Deposit:
    account_id: str
    amount: int

@process.defn(key="account_id")
@dataclass
class Balance:
    total: int = 0

    @process.event
    async def apply(self, event: Deposit):
        self.total += event.amount
        return {"account_id": event.account_id, "balance": self.total}
```

No topology builder. No separate state database. No recovery code in your application.

## Install Highwater

```bash
pip install highwater
```

## Run locally

```bash
highwater dev app.py
```

`highwater dev` starts a complete local environment, discovers the Processes in `app.py`, and prints the local ingestion endpoint. Storage, partitions, leases, and execution pools use development defaults.

Send an event from Python:

```python
from highwater import Client

client = Client()
balances = client.process("Balance")
await balances.send(
    Deposit("account-a", 5),
    event_id="deposit-1001",
)
```

Or send JSON to the generated event endpoint:

```bash
curl -X POST http://localhost:7233/v1/processes/Balance/events \
  -H 'content-type: application/json' \
  -H 'idempotency-key: deposit-1001' \
  -d '{"account_id":"account-a","amount":5}'
```

## Deploy

```bash
highwater deploy app.py
```

Highwater packages the application, creates a versioned deployment, provisions event ingestion, and scales execution independently for each state partition. The same Process API runs continuously, on demand, or on a schedule.

```bash
highwater deploy app.py --schedule '0 * * * *'
```

A schedule controls when compute drains available events. It does not turn off ingestion or weaken durability.

## Why Highwater

Traditional stream processors are good at dataflow graphs. Durable execution systems are good at long-running application code. Highwater combines their strongest ideas around one abstraction: a durable Process keyed by the entity your application already understands.

```text
events ──► durable inbox ──► Python Process ──► state + output
              per key          retryable          atomic
```

- One key runs one state transition at a time.
- Different keys scale independently.
- Accepted events survive executor and host failures.
- State and output commit together.
- Stable event identifiers make uncertain retries safe.
- Watermarks let code wait for event-time completeness.
- Backpressure reaches ingestion before queues become unbounded.

## Streaming that can be batchy

Highwater continuously batches transport and durable commits. Application code stays event-oriented unless it opts into vectorized execution:

```python
@process.defn(key="document_id")
class Embeddings:
    @process.batch(max_size=128, max_delay=0.025)
    async def embed(self, documents: list[Document]):
        vectors = await model.embed([doc.text for doc in documents])
        return [
            {"document_id": doc.document_id, "embedding": vector}
            for doc, vector in zip(documents, vectors, strict=True)
        ]
```

The batch runs when it reaches 128 documents or its oldest document waits 25 milliseconds. Scheduled deployments use the same mechanism to drain finite bursts and scale back to zero.

## Event time without a dataflow language

```python
@process.defn(
    key="account_id",
    event_time="occurred_at",
    wait_until=process.complete,
)
@dataclass
class DailyBalance:
    total: int = 0
```

`process.complete` runs an event only after Highwater knows the input is complete through that timestamp. The platform owns source progress, idleness, late-data policy, and watermark coordination.

Highwater also provides native incremental filters, windows, deduplication, interval joins, and temporal as-of joins. Use them for common state machines and keep application-specific decisions in Python.

## Event ingestion

Every deployment receives managed HTTPS and SDK ingestion. Highwater assigns durable source positions, validates idempotency keys, partitions by Process key, and applies admission backpressure.

Customers publish events to Highwater. Connectors, brokers, storage tiers, and partition movement are platform concerns rather than application configuration.

## Execution model

Highwater groups keyed Processes into movable partitions. Each partition pipelines and group-commits state transitions, keeps hot state close to execution, and snapshots durable progress asynchronously. Execution containers are cached according to observed traffic and can scale to zero when idle.

Cross-partition messages carry causal commit dependencies. A receiver can begin speculative work, but it cannot commit a result before the sender's dependency is durable. This avoids a distributed transaction on every message while preserving recovery order.

Leases control where an invocation may run. A lease token is a renewable capability tied to a durable partition generation. Expiration only makes a lease eligible for revocation; a durable generation change fences the old executor. Late completions from a prior generation cannot commit.

These choices follow the partitioned, pipelined execution model described by Microsoft Research's [Netherite](https://www.microsoft.com/en-us/research/publication/netherite-efficient-execution-of-serverless-workflows/) and the workload-aware warm execution findings from [Serverless in the Wild](https://www.microsoft.com/en-us/research/publication/serverless-in-the-wild-characterizing-and-optimizing-the-serverless-workload-at-a-large-cloud-provider/).

## Delivery guarantees

| Boundary | Guarantee |
| --- | --- |
| Event admission | acknowledged after a durable append |
| Per-key execution | ordered, one committed transition at a time |
| Invocation | at least once across failures |
| State and output | atomic within one Process transition |
| Event retry | idempotent with a stable event identifier |
| Output delivery | at least once with a stable message identifier |
| Event-time progress | monotonic per input partition |

Direct, non-idempotent external side effects can still occur more than once when an invocation fails after the effect. Use a destination idempotency key or Highwater's transactional output delivery.

## Performance

The current execution engine has completed 100,000 distinct durable keyed transitions at **108,164 events per second** on one development machine. Every event and completion used the durable commit path.

The architecture scales by moving independent state partitions across execution hosts. One hot key remains serial by design; split the key or use a commutative aggregation when one entity needs internal parallelism.

## Documentation

The Docusaurus site lives in [`website`](website/README.md).

```bash
cd website
yarn install
yarn start
```

The public documentation covers durable Processes, managed event ingestion, event time, batching, scheduled deployments, joins, scaling, backpressure, upgrades, recovery, delivery guarantees, and lease fencing.

## Repository

The repository contains the execution engine, Python SDK, examples, benchmarks, and documentation source. Internal crate and module names remain implementation details behind the packaged CLI.

```text
crates/               execution and protocol implementation
src/temporal_code/    Python SDK implementation
examples/             streaming applications
benchmarks/           durable throughput benchmark
docs/                 implementation design notes
website/              public documentation
```

## Build the implementation

Contributors working on the engine can build and test from source. End users install `highwater` and use `highwater dev`; these commands are not part of the customer setup path.

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src python3 -m unittest discover -s tests -v
cd website && yarn build
```
