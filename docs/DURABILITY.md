# Durability model

The durable source of truth is the partition journal, not local RocksDB and not the watermark. The control plane and keyed process lanes have independent sequences. A process key always hashes to the same lane, so its input, mailbox, lease, state, applied offset, output changelog, and outbox records never cross an atomic boundary. RocksDB is updated after journal commitment with its own WAL disabled because it is a disposable materialized cache.

Clustered mode writes each transition as an immutable S3 object, then advances one partition-head object with an ETag conditional write. The head update is the linearization point. S3 provides strong read-after-write consistency and storage replication; competing or stale owners receive a failed precondition. An uncertain response is resolved by rereading the head and comparing the proposed record identity. Filesystem mode retains the same record encoding but is limited to single-node development.

WAL records have an explicit format version and named outer fields. Format 2 stores puts, deletes, and range deletes in compact columns; recovery still reads format 1 records. An unknown format fails startup instead of guessing. This keeps the high-throughput representation evolvable without making durable state dependent on Rust struct field order.

Process completion uses the same boundary. An executor leases several Process activations and returns several results, and the owning lane commits their state, input progress, output changelog, outbox entries, and task acknowledgements in one WAL record. If the commit fails, none of those completions are acknowledged. Each grant includes the partition owner epoch and a partition-wide activation sequence. Renewal and completion verify both against the current durable owner inside the lane transaction, so a delayed executor cannot commit after restart or reassignment.

Every per-key process state records:

- its state schema version and producing build;
- the applied input sequence and event time;
- the application state value.

A checkpoint is a consistent RocksDB snapshot at a vector of journal positions, one per lane. Its immutable files are uploaded before the current-checkpoint pointer advances conditionally. Recovery downloads the newest published checkpoint and independently follows each partition's immutable journal tail. The checkpoint contains mailboxes, leased/running executions, ready indexes, per-key applied positions, watermarks, timers, source cursors, output state, and fenced ownership epochs. Changing the configured lane count for existing durable state is rejected.

## Failure behavior

| Failure point | Recovery behavior |
| --- | --- |
| Before an input WAL batch is synced | The producer was not acknowledged and retries the batch. Stable event IDs deduplicate an uncertain response. |
| After input acknowledgement, before execution | The admitted mailbox and source cursor replay from the WAL. |
| During a Python batch | No completion was committed; the runtime renews live work, while expiry makes abandoned work eligible for transactional revocation and retry. |
| Service restarts with active batches | Startup installs a higher durable owner epoch and immediately requeues old-epoch activations. Delayed renewal and completion requests fail closed. |
| During a completion commit | The whole WAL record is present or absent. Partial state/output transitions are never applied. |
| After a checkpoint but before manifest publication | Recovery uses the prior published checkpoint and replays more WAL. |
| Loss of local RocksDB | Restore the object checkpoint and replay its WAL tail. |

The recovery drill deletes the local RocksDB directory after a completed process transition, restarts with only the object directory, and verifies the exact per-key application state after WAL replay. The throughput result in the README was produced with the same acknowledgement path.

Watermarks only describe event-time completeness. They are checkpointed with state, but cannot replace input sequences: concurrent keys can complete out of sequence, and recovery must know which exact records produced each key's state.

## External effects

Process outputs enter a transactional outbox in the same completion commit. Delivery is at least once with deterministic message IDs. A sink obtains effectively exactly-once application by enforcing uniqueness on that ID or by participating in a checkpoint-aware transaction. Calling a non-idempotent external service directly from a batch handler is outside this guarantee.

## Trust boundary

Filesystem mode is crash durable only while its configured durable directory survives. Clustered mode requires a versioned S3 bucket, conditional-write permissions, least-privilege workload credentials, and an encrypted internal network. Compute hosts and local RocksDB directories are disposable. Object-store loss, credential compromise, deliberate deletion, or a correlated loss outside the bucket's durability contract remain disaster-recovery concerns.

Application sandboxes receive only a deployment-scoped execution capability. They do not receive journal, checkpoint, ownership, or cloud credentials. See [Execution isolation](EXECUTION_ISOLATION.md).
