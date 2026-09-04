# Deterministic process and output simulation

The `seeded_process_histories_preserve_outcomes_and_make_progress` test drives
the real admission, polling, completion, lease recovery, output promotion, sink
polling, and acknowledgement functions. It uses local RocksDB/WAL storage and
reopens it under a new runtime identity to exercise recovery and owner fencing.

A thread-local test clock advances lease expiry and retry availability without
sleeping. It is compiled only for tests and restored when the fixture is dropped.
The simulation runs synchronously, including its current-thread async sink calls;
the clock override does not propagate to other threads.

Each history admits twelve identified events across three keys. An independent
reference tracks each event's status and consumed attempts, plus each key's
committed counter. The generated failure prefix includes:

- Repeated identical admissions, including after terminal outcomes.
- Returned handler failures and mixed success/failure within a batch.
- Lease expiry followed by recovery and rejection of late completion.
- Runtime restart with leased work, epoch change, and orphan recovery.
- Successful completion followed by the same completion again.
- A malformed final batch item after earlier transitions have been staged.

After each decision, checks compare durable outcomes, attempt counts, per-key
state, completion/failure counts, and pending output IDs against the reference.
An event cannot overtake earlier pending events for its key or receive stale
prior state. Failed completion batches must leave the reference state unchanged.
Once the finite failure prefix ends, the workload must reach terminal outcomes
within a bounded number of poll cycles, without leaked key or concurrency permits.
This checks progress for these schedules; it is not a general fairness proof.

Output validation then exercises the actual sink endpoints. A wrong consumer's
acknowledgement must fail. A restart before acknowledgement must redeliver the
same message ID and payload. Duplicate acknowledgement is tolerated. Repeated
promotion and a final restart must not resurrect acknowledged messages. Delivered
events must match the reference's committed events exactly after destination-side
identity tracking.

## Run

```sh
# 32 histories, automatically included in the Rust workspace suite.
cargo test -p highwater-server seeded_process_histories -- --nocapture

# 128 histories, also configured for manual reliability CI.
HIGHWATER_PROCESS_SEED_COUNT=128 cargo test -p highwater-server \
  seeded_process_histories -- --nocapture

# Replay the seed and action trace printed in a failure log.
HIGHWATER_PROCESS_SEED=7 cargo test -p highwater-server \
  seeded_process_histories -- --nocapture
```

## Scope

Local validation on 2026-09-04: all 32 seeded histories passed, covering 384 unique
admitted events plus repeated admission and completion attempts. The complete Rust
workspace passed 31 tests; the process histories took approximately 34 seconds
within that run. Clippy, formatting, workflow syntax, and diff checks passed.
The configured 128-history manual expansion was not run locally in this pass.

This extends [journal simulation](JOURNAL_SIMULATION.md) upward into process
semantics. It uses one data partition, three keys, direct ingestion, a synchronous
operation schedule, and local storage. Storage failures during a process commit,
remote multi-node contention, stream-fed execution, watermark recovery, build
migration, and unconstrained scheduling are not modeled here. Existing journal,
Python integration, and MinIO chaos tests provide separate evidence for some of
those boundaries. Reopening durable storage is an orderly test restart, not
instruction-level process termination.
