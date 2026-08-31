# Scaling architecture

The programming model should remain one durable keyed Process even as execution moves across machines. Distribution is an implementation detail below that contract: a key has one ordered mailbox, one active transition, bounded admission, and an atomic input/state/output commit.

## Current data plane

Each key hashes to a WAL lane. A lane is a serial durability boundary, while different lanes append and execute concurrently. One versioned WAL record can atomically admit thousands of inputs or complete a leased batch. The record is synced before acknowledgement, then applied to RocksDB with its local WAL disabled. RocksDB provides indexed reads and checkpoint files; it is not the recovery authority.

Ready executions carry their input and prior state, and leases carry the complete activation batch. An executor crash therefore needs no reconstruction from transient application memory. Success commits state, output, lease removal, active-key release, and the next mailbox dispatch together. Failure or lease revocation leaves the prior state intact and makes the same durable input runnable again. Application execution already spreads across independent processes or hosts using disjoint partition assignments; every renewal and completion is fenced by the current durable partition-owner epoch and activation sequence.

Checkpoints publish a vector of per-lane positions before deleting covered WAL objects. Controlled compaction follows checkpoint publication, bounding file count and read amplification without putting compaction on every acknowledgement path.

## Multi-host data plane

The service-side state-machine owners currently share one process. The next architecture makes those owners independently placeable while keeping the implemented external execution assignment and replacing the storage and ownership adapters:

1. A shard leader appends to a three-replica log with quorum acknowledgement, or to an object store using conditional creation and a fenced compare-and-swap head.
2. A monotonically increasing ownership epoch is present on every append, lease, checkpoint, and completion. A stale owner cannot acknowledge work after reassignment.
3. Each owner keeps ready, mailbox, active-key, timer, and deduplication indexes in memory and in local RocksDB. Recovery loads an immutable checkpoint and replays the authoritative log tail.
4. Checkpoints upload immutable changed files, publish a vector manifest with compare-and-swap, then garbage-collect only objects covered by a published manifest.
5. Credit-based admission propagates capacity from execution pools through shard owners to producers. Overload delays or rejects admission before a source cursor is committed.

Adding machines then means reassigning fenced key groups and their checkpoint handles, not changing user code or introducing a topology API. A 10x load increase is handled by more independently owned lanes until a single hot key becomes the limit; one key remains intentionally serial to preserve its state semantics.

## Safety bar

Performance work may change encoding, batching, caches, or transport, but not these invariants:

- no acknowledgement before an authoritative durable append;
- no partial input/state/output completion;
- stable event IDs deduplicate an uncertain producer retry;
- durably revoked invocation leases rerun from committed prior state;
- checkpoint publication precedes WAL truncation;
- unknown durable formats and stale ownership epochs fail closed.

Before claiming multi-host durability, validate process death, host loss, owner fencing, truncated writes, delayed duplicate completions, checkpoint interruption, object-store timeout, and local-state deletion under continuous load. Throughput without those failure drills is only a speed result.
