# Highwater

Durable execution for streaming applications.

Highwater lets you write stateful stream processing as ordinary Python. It keeps each key ordered, persists every accepted event and state transition, tracks event-time progress, retries failed invocations, and scales execution with load.

```python
from dataclasses import dataclass
from highwater import streaming

@dataclass(frozen=True)
class Deposit:
    account_id: str
    amount: int

@streaming.process(key="account_id")
@dataclass
class Balance:
    total: int = 0

    @streaming.event
    async def apply(self, event: Deposit):
        self.total += event.amount
        return {"account_id": event.account_id, "balance": self.total}
```

No topology builder. No separate state database. No recovery code in your application.

## Install Highwater

```bash
pip install highwater
```

The platform wheel includes the native streaming engine and the Python SDK.

## Run locally

```bash
highwater-server --state-dir .highwater/state --object-store-dir .highwater/objects
```

In another terminal, register the application code with the local execution service:

```bash
highwater-worker app
```

`highwater-server` starts the durable service with local storage. The worker discovers the Processes and workflows in `app.py`.

The same CLI executes against Highwater Cloud:

```bash
export HIGHWATER_ADDRESS=https://api.highwater.cloud
export HIGHWATER_API_KEY=...
highwater example run account-balance
highwater example run order
```

Send an event from Python:

```python
from highwater import Client

client = Client()
balances = await client.get_process_handle("Balance")
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

## Execute remotely

Workers and the durable service are deployed independently, as they are in Temporal. Point the CLI or Python client at the service address; workflow and Process commands are otherwise identical to local commands.

```bash
highwater --address "$HIGHWATER_ADDRESS" workflow start \
  --type OrderWorkflow \
  --arg '"4242424242424242"' \
  --arg '25' \
  --wait
```

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
- Poison events use isolated retry capacity and enter durable quarantine after their retry budget.

## Streaming that can be batchy

Highwater continuously batches transport and durable commits. Application code stays event-oriented unless it opts into vectorized execution:

```python
@streaming.process(key="document_id")
class Embeddings:
    @streaming.batch(max_size=128, max_delay=0.025)
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
@streaming.process(
    key="account_id",
    event_time="occurred_at",
    wait_until=streaming.complete,
)
@dataclass
class DailyBalance:
    total: int = 0
```

`streaming.complete` runs an event only after Highwater knows the input is complete through that timestamp. The platform owns source progress, idleness, late-data policy, and watermark coordination.

Highwater also provides native incremental filters, windows, deduplication, interval joins, and temporal as-of joins. Use them for common state machines and keep application-specific decisions in Python.

Versioned reference streams can be read directly from Process code. Highwater waits until the reference stream is complete through the event timestamp, then resolves the latest version at or before that time.

```python
from dataclasses import dataclass, field

catalog = streaming.versioned("catalog", key="product_id")

@streaming.process(key="user_id")
@dataclass
class ShoppingAssistant:
    recent: list[str] = field(default_factory=list)

    @streaming.event
    async def recommend(self, view, context):
        product = await catalog.get(
            view.product_id, as_of=context.event_time)
        self.recent.append(product.category)
        return await recommendation_model.rank(
            product=product, recent=self.recent)
```

## Event ingestion

Every deployment receives managed HTTPS and SDK ingestion. Highwater assigns durable source positions, validates idempotency keys, partitions by Process key, and applies admission backpressure.

Customers publish events to Highwater. Connectors, brokers, storage tiers, and partition movement are platform concerns rather than application configuration.

The Wikimedia example consumes the public recent-change feed in microbatches and maintains durable per-page activity state:

```bash
python -m examples.wikimedia_recent_changes --target "$HIGHWATER_ADDRESS"
```

Its upstream `Last-Event-ID` is committed atomically with each Highwater batch. A restart resumes from the last acknowledged public event rather than from process memory. The terminal Process prints each event and discards successful input and activation history; only its bounded per-wiki counters and source checkpoint remain durable. Use `--duration 60` for a bounded run.

## Execution model

Highwater groups keyed Processes into movable partitions. Each partition pipelines and group-commits state transitions, keeps hot state close to execution, and snapshots durable progress asynchronously. Execution containers are cached according to observed traffic and can scale to zero when idle.

Cross-partition messages carry causal commit dependencies. A receiver can begin speculative work, but it cannot commit a result before the sender's dependency is durable. This avoids a distributed transaction on every message while preserving recovery order.

Leases control where an invocation may run. A lease token is a renewable capability tied to a durable partition generation. Expiration only makes a lease eligible for revocation; a durable generation change fences the old executor. Late completions from a prior generation cannot commit.

These choices follow the partitioned, pipelined execution model described by Microsoft Research's [Netherite](https://www.microsoft.com/en-us/research/publication/netherite-efficient-execution-of-serverless-workflows/) and the workload-aware warm execution findings from [Serverless in the Wild](https://www.microsoft.com/en-us/research/publication/serverless-in-the-wild-characterizing-and-optimizing-the-serverless-workload-at-a-large-cloud-provider/).

## Elastic execution

Application workers can own disjoint Process partitions. A scale-out changes the assignment while durable owner epochs fence stale completions. Keys continue from committed state when their partition moves.

`highwater-autoscaler` samples admitted work, completed work, and backlog, then prints a bounded replica decision with balanced partition assignments:

```bash
highwater-autoscaler \
  --process shopping-assistant \
  --target "$HIGHWATER_ADDRESS" \
  --current-replicas 4 \
  --partitions 64
```

The command produces a controller input. Kubernetes, Highwater Cloud, or another scheduler applies the replica count and starts workers with the returned partition assignment.

In Kubernetes, the controller can watch the Process and update a worker Deployment directly:

```bash
highwater-autoscaler \
  --process shopping-assistant \
  --target "$HIGHWATER_ADDRESS" \
  --partitions 64 \
  --watch \
  --kubernetes-deployment shopping-assistant-workers
```

## Compare an application build

Retained Process input can be replayed through two decorated Process classes before activation. The comparison reports every state or output difference and writes neither result to the running deployment.

```python
comparison = await client.compare_builds(
    "shopping-assistant",
    baseline=ShoppingAssistantV1,
    candidate=ShoppingAssistantV2,
)

for difference in comparison.differences:
    print(difference.event_id, difference.baseline_output, difference.candidate_output)
```

Versioned stream reads resolve from retained history at the original event time. Application code runs during comparison, so calls to external systems should use recorded responses or an evaluation implementation.

## Hosted and sandboxed workers

The public API and private execution API can listen on separate network interfaces. Execution tokens are scoped to a task queue and an allowlist of build IDs. A worker cannot use its execution identity to reach the public data API.

[`deploy/sandbox/worker.yaml`](deploy/sandbox/worker.yaml) provides the worker boundary. [`deploy/kubernetes/hosted.yaml`](deploy/kubernetes/hosted.yaml) composes sharded core services, conditional object storage, private execution endpoints, sandboxed workers, autoscalers, disruption budgets, and network policy. The worker profile uses gVisor, a non-root user, a read-only root filesystem, no Linux capabilities, a seccomp profile, bounded processes and compute, disabled service-account credentials, and default-deny ingress and egress.

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

The representative benchmark sends 100,000 product views across 20,000 shopping sessions and 10,000 products. It updates bounded durable session state, performs ranking work, and emits a recommendation every fifth view. A measured development-machine run completed **53,638 events per second** with 20 execution instances. Hardened container workers reached **30,608 events per second** on the same machine. The minimal counter workload reached **65,974 events per second** through the container boundary. Every admission and completion is acknowledged only after its authoritative WAL append.

Execution instances and partition state-machine owners can run on separate hosts. Clustered deployments linearize each partition through a conditionally updated object-store head, route ingestion to its current owner, and move ownership through a checkpoint-plus-tail handoff. Durable owner epochs and activation sequences fence delayed work throughout restart or reassignment. See [Scaling architecture](docs/SCALING.md).

See [Performance](docs/PERFORMANCE.md) for the reproducible benchmark, scaling results, and measurement boundary. One hot key remains serial by design; split the key or use a commutative aggregation when one entity needs internal parallelism.

## Documentation

The Docusaurus site lives in [`website`](website/README.md).

```bash
cd website
yarn install
yarn start
```

The public documentation covers durable Processes, managed event ingestion, event time, batching, scheduled deployments, joins, scaling, backpressure, upgrades, recovery, delivery guarantees, and lease fencing.

The standalone product landing page lives in [`landing`](landing/README.md). Its static files can be previewed directly and deployed to an S3 origin without an application server.

## Repository

The repository contains the execution engine, Python SDK, examples, benchmarks, and documentation source. Internal crate and module names remain implementation details behind the packaged CLI.

```text
crates/               execution and protocol implementation
src/highwater/    Python SDK implementation
examples/             streaming applications
benchmarks/           durable throughput benchmark
docs/                 implementation design notes
website/              public documentation
landing/              product landing page
```

## Build the implementation

Contributors working on the engine can build and test from source. End users install the platform wheel and run `highwater-server`.

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src python3 -m unittest discover -s tests -v
cd website && yarn build
```
