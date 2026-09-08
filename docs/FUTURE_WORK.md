# Future work

This roadmap records remaining work after the latency, finality, saved-result, and
replay-manifest additions. Proposed capabilities below are not current guarantees.

## Durability and distribution priorities

Durability and distribution take precedence over expanding the operator catalog.
Keep this order:

1. Run and extend the existing opt-in S3 failure suite around ambiguous head writes, checkpoint publication, source death, and target activation. Include key finalization and saved-result capture under ownership changes.
2. Add checkpoint-vector-covered garbage collection for immutable pending output markers and remote journal history; current retention is deliberately conservative.
3. Add automatic partition placement and load-aware movement on top of the implemented manual handoff.
4. Move invocation and cluster transport from HTTP/JSON to the versioned Protobuf protocol with streaming RPC.
5. Complete Process build pinning and migration compatibility across rolling deployments.
6. Add a configurable maximum total invocation duration; durable revocation, runtime renewal, and owner-epoch fencing are implemented.
7. Add sticky Process caches and extend group commit to every transition type.
8. Materialize large Arrow IPC payloads in object storage and keep references in history.
9. Expand nondeterminism, observability, and history-inspection coverage.

Do not add more language SDKs or a cluster control plane until the first four items are complete.

## Streaming operator follow-ups

Native normalization, general retractable top-N, indexed collection outputs, and
scoped changelog capability validation are implemented; see
[collections](COLLECTIONS.md). Remaining shared-model work includes migration of
legacy operators, consolidated weights, richer time/progress policies, bounded
edge scheduling, and execution on partition owners.


The [changelog workflow assessment](CHANGELOG_WORKFLOWS.md) maps Flink's streaming
operator families to a proposed shared contract and gives the implementation
sequence for native retractable top-N. It identifies mode validation, row/time
identity, normalization, and shared arrangements as prerequisites for expanding
the relational operator catalog safely.

The fixed-window, temporal-join, bounded interval-join, and event-time keep-first deduplication operators now have durable state, deterministic Process outputs, and idempotent deployment. The remaining work is:

1. Move keyed operator execution to remote owners; keys and input writes already carry fenced key-group epochs.
2. Add sink-specific adapters. Output delivery is at least once; effectively exactly-once application requires destination deduplication by message ID or a transactional sink protocol.
3. Add schema-declared composite primary keys; full changelog row kinds and signed operator differences are implemented.
4. Add checkpoint size, state-retention, late-record, watermark-lag, and backpressure metrics.
5. Add credit-based edge backpressure, unaligned checkpoints, partition-local checkpoint handles, and rescaling transfer; native exactly-once edges, full-state journal-vector checkpoints, and retractable interval-join arrangements are implemented.
6. Move keyed process mailboxes and concurrency permits with their key groups during rescaling; local keyed isolation, capacity backpressure, and completeness gates are implemented.

## Result-contract roadmap

### Current baseline

| Area | Implemented | Current boundary |
| --- | --- | --- |
| Latency | Per-Process batch deadline caps, retained-outcome diagnostics, age-aware autoscaling | No end-to-end dependency budget or hard deadline guarantee |
| Finality | Source-policy explanation and durable closure of quiescent direct-ingress keys | No arbitrary business predicates, reopening, or automatic state reclamation |
| Maintained results | Native changelog projection and durable saved snapshots | Capture scans history; shared cuts require same-source filters with matching historical input counts |
| Replay | Portable manifests with inputs, reference histories, initial state, and build IDs | Separate live reads; no external-response recording or full execution branch |

See [streaming semantics](STREAMING_MODEL.md), [scaling](SCALING.md),
[maintained results](MAINTAINED_RESULTS.md), and [replay](REPLAY.md) for the current
API contracts.

### 1. Bound the cost of inspection and saved reads

Implement this before encouraging frequent polling or large result snapshots.

- Maintain latency counters and rolling distributions incrementally. Index the
  oldest unfinished admission across normal and retry lanes, preserving its age
  through worker loss and reassignment.
- Maintain native result rows in the same transaction as their changelog. Add
  keyed reads, pagination, and byte limits so reading one key does not fold an
  entire history or create a durable snapshot.
- Bootstrap indexes for existing deployments from a defined history cut, then
  replay the tail without missing or double-applying changes.
- Add snapshot listing, idempotent capture request IDs, and explicit expiry and
  retention policies. Define what happens when a reader uses an expired token.

**Completion criteria:** inspection cost no longer grows with completed history;
indexed rows match changelog reconstruction through updates, deletes, and
recovery; ambiguous capture retries return the same snapshot. Measure admission
latency while capturing large results to establish an acceptable overhead.

### 2. Extend coherent reads beyond same-source filters

Define a read token containing an input-position vector and dependency coverage.
Track which inputs have been incorporated by each operator or Process, retain
versions needed by live tokens, and publish a cut only when all selected results
cover it. A recovery checkpoint vector alone does not establish this guarantee.

Expand in stages: native operators over multiple inputs, connected native
operators, then asynchronous Process outputs. Replace historical input-count
checks with explicit coverage metadata as the supported lifecycle expands.

**Completion criteria:** readers cannot combine numerator and denominator values
from different input cuts; delayed branches, retries, backfills, and ownership
changes preserve the guarantee. Unsupported or unavailable cuts fail explicitly.
Do not describe event-time `as_of` selection as transaction snapshot isolation.

### 3. Allocate latency and freshness budgets across dependencies

Keep three measurement boundaries distinct:

- Input ingestion to committed Process output: execution latency.
- Source progress to published result progress: derived-result freshness.
- Output commitment to destination acknowledgement: delivery latency.

Allow consumers to declare targets and propagate the strictest downstream demand
through supported dependency graphs. Allocate remaining budget across queueing,
batching, execution, and delivery rather than giving every stage the entire
target. Use observed service times and worker startup costs to inform warm
capacity and scaling decisions.

**Completion criteria:** multi-stage load tests report end-to-end target misses
and explain whether source stalls, hot keys, compute, or delivery caused them.
Compare resource cost and tail latency against fixed batching. No target may
bypass ordering, completeness gates, retry policy, or durable acknowledgement.

### 4. Generalize business finalization carefully

Extend the current key-closure primitive to declared immutable regions, such as
an approved billing period. Define scope, authority, and treatment of later
changes before adding predicate syntax. Persist declarations atomically with
admission checks and propagate them only through operators that can prove the
corresponding output region is final.

Treat corrections as a separately specified operation. If reopening is needed,
define a new revision or generation; do not silently revoke an existing finality
promise. State cleanup must also respect replay retention, active read tokens,
pending output, and published checkpoint coverage.

**Completion criteria:** concurrent finalization and admission have a single
durable outcome; restart and reassignment cannot admit forbidden changes;
identical retries remain valid. Tests must distinguish final state from completed
external delivery.

### 5. Build reproducible execution branches

First capture replay input, initial state, and reference histories at a coherent
cut using the read-token protocol above. Pin code artifacts and dependency
versions in addition to build labels. Add an opt-in recording boundary for
external responses, time, and randomness, with explicit behavior when a recording
is missing or incompatible.

Only then add isolated branches that can replay timers, retries, and feedback.
Branch identifiers must scope state and pending effects; comparison branches
must not deliver production output. Define retention, deletion, and resource
limits before exposing long-lived branches. Promotion requires its own migration
and conflict semantics and is not implied by replay support.

**Completion criteria:** the same captured execution produces the same state and
output after restart without consulting live dependencies. Missing recordings
fail explicitly, and branch execution cannot mutate the running deployment.

## Delivery order and review requirements

Within the result-contract work, start with bounded inspection and indexed reads,
then coherent cuts. Dependency-wide budgets can follow once progress and latency
measurements are reliable; full execution branches depend on coherent capture.
General finalization should use narrow business cases to establish semantics
before adding a general predicate language.

For each addition, document its consistency boundary, retention cost, unsupported
cases, and measured performance. Add failure tests for new durable state and
protocols. Keep Python Process code as the primary application interface; a SQL
surface, general dataflow builder, or additional language SDK is not required by
this roadmap.
