# Highwater TLA+ models

`JournalOwnership.tla` models partition ownership, activation grants, prepared
completions, takeover, the journal-head compare-and-swap, epoch fencing, and
event deduplication.

`CheckpointBarrier.tla` models the relationship between a barrier's exact
partition-position vector and the cuts represented by acknowledged state
handles. Its epoch-only mutation is expected to produce a counterexample.

`JournalVectorCheckpoint.tla` models the full-state journal-vector checkpoint
protocol used by the implementation: exact snapshot cuts, conditional pointer
publication, tail replay, and cleanup only through published positions.

`ExecutionLifecycle.tla` models activation grants, successful completion,
handler failure, lost leases, terminal failure, durable output delivery, and
acknowledgement. Its unsafe configuration demonstrates that a crash loop can
violate eventual termination when lost activations do not consume a bounded
attempt budget.

The normal configurations must pass every invariant and temporal property. The
`Unsafe*` configurations are mutation checks: TLC is expected to reject them with a
counterexample. If an unsafe configuration passes, the model or its invariants
are too weak.

The model is deliberately bounded. Passing TLC means every state within these
bounds was explored under the modeled actions and assumptions; it is not a
proof that the Rust implementation refines this specification.

## Run

Download the official TLA+ tools JAR, then run from this directory:

```bash
java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalOwnership.cfg JournalOwnership.tla
```

The validation on 2026-09-02 used TLA+ tools release 1.8.0 with SHA-256:

```text
dbcc75552f21978a4846688b8e23be1a6b6c0b3fcee35d78fec2df167958ec94
```

Mutation checks should exit nonzero and print a counterexample:

```bash
java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalOwnership_UnsafeFencing.cfg JournalOwnership.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalOwnership_UnsafeDedup.cfg JournalOwnership.tla
```

The checkpoint configurations run similarly:

```bash
java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config CheckpointBarrier.cfg CheckpointBarrier.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config CheckpointBarrier_EpochOnly.cfg CheckpointBarrier.tla
```

The implemented journal-vector protocol and its mutations run with:

```bash
java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalVectorCheckpoint.cfg JournalVectorCheckpoint.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalVectorCheckpoint_UnsafeCleanup.cfg \
  JournalVectorCheckpoint.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config JournalVectorCheckpoint_UnsafeRegression.cfg \
  JournalVectorCheckpoint.tla
```

The end-to-end execution lifecycle and lease-loss mutation run with:

```bash
java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config ExecutionLifecycle.cfg ExecutionLifecycle.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config ExecutionLifecycle_UnsafeLeaseLoss.cfg \
  ExecutionLifecycle.tla

java -XX:+UseParallelGC -jar /path/to/tla2tools.jar \
  -deadlock -config ExecutionLifecycle_UnsafeOutputCleanup.cfg \
  ExecutionLifecycle.tla
```

`-deadlock` disables TLC's deadlock check. The ownership and checkpoint models
check bounded safety; the execution model also checks weak-fairness liveness.
States that exhaust configured bounds are expected terminal states rather than
protocol deadlocks.
