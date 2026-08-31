# Durability model

The durable source of truth is the immutable object WAL, not local RocksDB and not the watermark. The control plane and keyed process lanes have independent WAL sequences. A process key always hashes to the same lane, so its input, mailbox, lease, state, applied offset, output changelog, and outbox records never cross an atomic boundary. Concurrent requests are group-committed within each lane and acknowledged only after the WAL object and directory entry are synced. RocksDB is then updated without its own WAL because it is a disposable materialized cache that startup can reconstruct.

WAL records have an explicit format version and named outer fields. Format 2 stores puts, deletes, and range deletes in compact columns; recovery still reads format 1 records. An unknown format fails startup instead of guessing. This keeps the high-throughput representation evolvable without making durable state dependent on Rust struct field order.

Process completion uses the same boundary. An executor leases several Process activations and returns several results, and the owning lane commits their state, input progress, output changelog, outbox entries, and task acknowledgements in one WAL record. If the commit fails, none of those completions are acknowledged.

Every per-key process state records:

- its state schema version and producing build;
- the applied input sequence and event time;
- the application state value.

A checkpoint is a consistent RocksDB snapshot at a vector of WAL positions, one per lane. Checkpoint creation holds every lane sequence while RocksDB captures the snapshot, then publishes the manifest atomically. Recovery restores the newest published checkpoint and independently replays each lane after its recorded position. The checkpoint contains mailboxes, leased/running executions, ready indexes, per-key applied positions, watermarks, timers, source cursors, output state, and fenced ownership epochs. Changing the configured lane count for existing durable state is rejected.

## Failure behavior

| Failure point | Recovery behavior |
| --- | --- |
| Before an input WAL batch is synced | The producer was not acknowledged and retries the batch. Stable event IDs deduplicate an uncertain response. |
| After input acknowledgement, before execution | The admitted mailbox and source cursor replay from the WAL. |
| During a Python batch | No completion was committed; durable leases expire, the ready index is reconstructed, and the inputs run again. |
| During a completion commit | The whole WAL record is present or absent. Partial state/output transitions are never applied. |
| After a checkpoint but before manifest publication | Recovery uses the prior published checkpoint and replays more WAL. |
| Loss of local RocksDB | Restore the object checkpoint and replay its WAL tail. |

The recovery drill deletes the local RocksDB directory after a completed process transition, restarts with only the object directory, and verifies the exact per-key application state after WAL replay. The throughput result in the README was produced with the same acknowledgement path.

Watermarks only describe event-time completeness. They are checkpointed with state, but cannot replace input sequences: concurrent keys can complete out of sequence, and recovery must know which exact records produced each key's state.

## External effects

Process outputs enter a transactional outbox in the same completion commit. Delivery is at least once with deterministic message IDs. A sink obtains effectively exactly-once application by enforcing uniqueness on that ID or by participating in a checkpoint-aware transaction. Calling a non-idempotent external service directly from a batch handler is outside this guarantee.

## Current trust boundary

The filesystem object-store implementation is crash durable when its directory resides on durable storage with the promised `fsync` semantics. WAL lanes execute in parallel, but they are not yet replicated across servers. Losing both the local RocksDB directory and the configured object-store directory loses the execution.

For company-grade high availability, the same WAL interface must be backed by either a quorum-replicated shard log or a conditional-write object store with a replicated low-latency ingress log. A three-replica/two-ack shard log can change the failure boundary without changing process, batching, checkpoint, or recovery semantics. Until that adapter and automated failover exist, the correct claim is single-node crash durability, not cluster high availability.
