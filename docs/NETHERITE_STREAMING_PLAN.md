# Netherite-aligned streaming execution plan

## Objective

Evolve Highwater from sharded durable storage with external execution into partition-owned durable streaming runtimes. The target preserves Highwater's Process API while adopting Netherite's strongest execution techniques: independently owned partitions, pipelined persistence, speculative execution with causal commit dependencies, asynchronous snapshots, and tiered hot state.

This plan does not add customer-facing infrastructure concepts. Partitions, logs, ownership, checkpoint handles, and execution placement remain platform implementation details.

## Implemented milestone

The partition-runtime and remote-ownership increments are complete:

- admission, activation polling, renewal, and completion enter one serialized command loop per local partition;
- each service incarnation installs a durable owner epoch, and restart recovery immediately requeues work from the superseded epoch;
- every activation and completion carries its partition, owner epoch, activation sequence, and lease token;
- completion and renewal revalidate that fence in the same partition transaction that changes durable state;
- execution instances can be assigned disjoint partition sets and run on separate hosts;
- the runtime renews active invocations without involving application code.
- partition journals use immutable S3 records and an ETag-conditional head as their linearization point;
- managed ingress routes to owners on other service instances over authenticated cluster transport;
- checkpoint-plus-tail handoff drains, fences, restores, and activates a partition at a higher epoch;
- application workers run as warm, deployment-scoped sandbox pools without storage credentials.

Automatic placement, streaming RPC transport, and end-to-end credit propagation remain. Checkpoint files are content-addressed and reused across snapshots. Manual placement and movement are the current correctness boundary; adding a balancer must not weaken it.

## Invariants

The implementation must preserve these properties throughout the migration:

- An event is acknowledged only after an authoritative durable append.
- One Process key has at most one committed transition at a time.
- Process state, input progress, output, and the next mailbox decision commit atomically.
- A stale partition owner or invocation cannot commit.
- Watermarks never advance beyond durably admitted input and visible operator output.
- Backpressure reaches ingestion before internal queues become unbounded.
- Checkpoint publication precedes deletion or tiering of covered history.
- Unknown durable formats, ownership epochs, and causal dependencies fail closed.

## Target architecture

Each virtual partition is an independently recoverable execution unit:

```text
                           ┌──────────────────────────────────────┐
ingestion ────────────────►│ partition command queue              │
invocation completions ───►│                                      │
timers and watermarks ────►│ ordered state-machine loop           │
cross-partition messages ─►│   ├── Process mailboxes              │
                           │   ├── operator and timer state        │
                           │   ├── ready invocations               │
                           │   └── transactional output            │
                           │                                      │
                           │ append pipeline ──► durable log       │
                           │ hot state       ──► local state cache │
                           │ checkpoints     ──► object storage    │
                           └──────────────────────────────────────┘
```

A partition has one durable owner epoch. It serializes state-machine decisions while allowing storage, application execution, checkpoint upload, and independent partitions to proceed concurrently.

## Phase 1: partition runtime (implemented locally)

Introduce a `PartitionRuntime` for each locally owned virtual partition.

### Responsibilities

- Own the partition command channel and state-machine loop.
- Assign monotonically increasing partition log positions.
- Hold ready queues, active-key permits, timers, watermarks, deduplication indexes, and output queues.
- Batch all mutation types into the partition append pipeline.
- Start application invocations only from committed or dependency-safe transitions.
- Stop accepting commands immediately when its owner epoch is fenced.

### Commands

The initial command set should include:

- `AdmitEvents`
- `CompleteInvocation`
- `AdvanceSourceProgress`
- `FireProcessingTimer`
- `ReceivePartitionMessage`
- `RenewInvocation`
- `RevokeInvocation`
- `BeginCheckpoint`
- `InstallOwnership`

Each command returns a future resolved at its documented boundary: admission durability, completion durability, or checkpoint publication.

### Migration

Route the current streaming endpoints through partition command channels. Retain existing durable record encodings during this phase. Remove global streaming scans and mutation locks only after every streaming mutation is partition-owned.

## Phase 2: pipelined persistence

Give each partition an append pipeline with three positions:

- **issued:** assigned to a transition;
- **flushed:** bytes reached the durable storage operation;
- **committed:** the authoritative store confirmed the append.

Batch by encoded bytes, record count, or a short latency deadline. Admission and completion futures resolve only at `committed`.

Application execution may overlap an in-flight append when its result remains fenced behind that append's commit position. This is speculation, not early acknowledgement.

### Failure behavior

- Failure before append commitment discards speculative descendants.
- An uncertain storage response is resolved by reading the fenced partition head.
- Recovery ignores records beyond the last valid committed head.
- A former owner cannot advance the head after a higher ownership epoch is installed.

## Phase 3: causal commit dependencies

Every internally emitted cross-partition message carries:

```text
message_id
source_partition
source_owner_epoch
source_log_position
payload
```

The tuple `(source_partition, source_owner_epoch, source_log_position)` is the message's commit dependency. A receiver may admit and speculatively execute the message, but cannot commit its effects until the source dependency is known durable.

### Dependency tracking

- Maintain a compact durable frontier of known committed positions per relevant source partition.
- Collapse multiple dependencies from one source to their maximum required position.
- Persist receiver deduplication and dependency state with the receiving transition.
- Reject a dependency from an unknown or superseded source epoch unless recovery can resolve it.

### Streaming applications

Use commit dependencies for Process messages, operator edges, window output, join changes, watermark propagation, and transactional output. Do not introduce distributed two-phase commit.

## Phase 4: invocation fencing and renewal (implemented locally)

Every activation contains:

```text
partition_id
partition_owner_epoch
activation_sequence
lease_token
lease_deadline
```

Grant, renew, revoke, and complete through the owning `PartitionRuntime`.

- Expiry makes an activation eligible for revocation; it does not itself change ownership.
- Renewal conditionally extends the same token, activation sequence, and owner epoch.
- Revocation durably removes the grant before requeueing its input.
- Completion validates every field in the same partition transition that commits the result.
- Partition reassignment fences all activations from older owner epochs.

The runtime, not application code, controls renewal cadence and maximum invocation duration.

## Phase 5: asynchronous incremental checkpoints

Replace coordinated full-state snapshots with partition checkpoint handles.

1. A partition chooses a committed log position.
2. It creates immutable state files without stopping later execution.
3. It uploads only files absent from the durable object tier.
4. It conditionally publishes a checkpoint handle containing partition ID, owner epoch, state files, format version, and covered log position.
5. History becomes eligible for tiering or deletion only after publication.

A deployment checkpoint manifest references compatible published partition handles. Globally aligned checkpoints remain available for operations that require a consistent multi-input cut, but they are not the normal recovery mechanism.

## Phase 6: tiered hybrid state

Introduce a state-store interface before replacing any local implementation:

```rust
trait PartitionStateStore {
    fn get(&self, key: &[u8]) -> StateRead;
    fn apply(&mut self, batch: StateBatch) -> Result<()>;
    fn checkpoint(&self, position: LogPosition) -> CheckpointFuture;
    fn install(&mut self, handle: CheckpointHandle) -> Result<()>;
}
```

The storage tiers are:

- memory for scheduling indexes and the hottest values;
- local disk for a rebuildable state cache;
- object storage for authoritative history and immutable checkpoint files.

Do not build a new storage engine in this phase. Preserve the existing local state implementation behind the interface, measure its limits, and replace components only when profiling justifies it.

## Phase 7: durable messaging and event-time progress

### Partition inboxes

The receiver deduplicates `message_id` and applies the message in its own partition log. Retries never require a distributed transaction.

### Watermarks

Represent watermark changes as causally durable control messages:

- source progress becomes visible after preceding source events are durably admitted;
- operator output watermarks wait for earlier output changes and their dependencies;
- a downstream combined watermark is the minimum of durable active-input frontiers;
- idleness and reactivation are versioned partition state;
- recovery cannot observe progress ahead of its data.

Reserve command and append capacity for progress messages so data backpressure cannot permanently prevent watermark advancement.

## Phase 8: credits, recovery, and movement

### Credit-based admission

Calculate partition credits from:

- uncommitted append bytes;
- durable inbox and mailbox occupancy;
- active invocation capacity;
- checkpoint upload lag;
- downstream output backlog;
- local state-cache pressure.

Propagate credits to managed ingestion. Do not acknowledge into an unbounded intermediate queue.

### Ownership transfer

The first implementation can use manual transfer:

1. stop new admission to the old owner;
2. commit or revoke active transitions;
3. publish a checkpoint handle and final log position;
4. conditionally install a higher owner epoch;
5. restore the checkpoint and replay the tail;
6. rebuild indexes and resume admission.

Automatic load balancing should follow correctness testing, not precede it.

## Validation gates

Each phase must pass deterministic fault injection at every durable boundary:

- process termination before and after append;
- uncertain object-store and quorum responses;
- delayed completion after activation revocation;
- partition reassignment during execution;
- duplicate and reordered cross-partition messages;
- crash during checkpoint upload and publication;
- loss of every local state file;
- watermark propagation concurrent with data backpressure;
- 10x offered load with bounded memory.

Track admission throughput, committed-transition throughput, group-commit size, storage operations per transition, speculative work discarded, checkpoint bytes uploaded, replay duration, watermark lag, p50/p99 latency, and backlog age.

## Recommended implementation order

1. Fault-inject the conditional journal and manual movement protocol.
2. Split content-addressed checkpoint manifests by partition.
3. Extend fenced ownership from Process partitions to every keyed operator mutation.
4. Move cluster and invocation traffic to streaming RPC.
5. Propagate credits across remote edges and ingestion.
6. Add tiered state optimization and automatic balancing.

Automatic placement follows the manual protocol; it does not introduce a second ownership mechanism.
