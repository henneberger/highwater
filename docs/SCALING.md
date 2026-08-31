# Scaling architecture

The programming model should remain one durable keyed Process even as execution moves across machines. Distribution is an implementation detail below that contract: a key has one ordered mailbox, one active transition, bounded admission, and an atomic input/state/output commit.

## Current data plane

Each key hashes to a WAL lane. A lane is a serial durability boundary, while different lanes append and execute concurrently. One versioned WAL record can atomically admit thousands of inputs or complete a leased batch. The record is synced before acknowledgement, then applied to RocksDB with its local WAL disabled. RocksDB provides indexed reads and checkpoint files; it is not the recovery authority.

Ready executions carry their input and prior state, and leases carry the complete activation batch. An executor crash therefore needs no reconstruction from transient application memory. Success commits state, output, lease removal, active-key release, and the next mailbox dispatch together. Failure or lease revocation leaves the prior state intact and makes the same durable input runnable again. Application execution already spreads across independent processes or hosts using disjoint partition assignments; every renewal and completion is fenced by the current durable partition-owner epoch and activation sequence.

Checkpoints publish a vector of per-lane positions before deleting covered WAL objects. Controlled compaction follows checkpoint publication, bounding file count and read amplification without putting compaction on every acknowledgement path.

## Multi-host data plane

Service-side partition owners are independently placeable. Each owner materializes its journal in local RocksDB and synchronizes control metadata before a partition transition. Managed ingestion reads the fenced owner record and forwards a batch over the authenticated cluster transport; customers continue using one ingestion endpoint.

Each partition append first creates an immutable record and then conditionally advances its S3 head. Ownership, invocation renewal, and completion use that same serial boundary. The owner epoch may stay constant or advance by exactly one, so delayed nodes cannot skip generations or commit against a stale ETag.

Migration follows a fail-closed handoff:

1. the source changes the partition from `ACTIVE` to `DRAINING`;
2. active grants are durably revoked and their inputs return to the ready queue;
3. all journal tails are synchronized and an immutable checkpoint is published;
4. a conditional append installs the target node at a higher epoch in `RESTORING`;
5. the target restores the checkpoint, replays the tail, and conditionally enters `ACTIVE`;
6. routing observes the new owner, while every old grant and completion remains fenced.

Warm targets may subscribe to an overlapping desired partition set before movement. They do not acquire an unexpired partition assigned elsewhere. A failed source becomes eligible for takeover only after lease expiry; time enables liveness, while the conditional epoch append provides safety.

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

The opt-in S3 chaos test exercises remote routing, hard owner loss, lease takeover, stale-completion fencing, checkpoint recovery after local-state deletion, and retry deduplication across an object-store outage. It starts two service processes and an isolated MinIO instance:

```bash
HIGHWATER_S3_CHAOS=1 PYTHONPATH=src python3 tests/test_s3_chaos.py -v
```

Set `HIGHWATER_S3_CHAOS_URI=s3://bucket/test-prefix` to run against AWS S3 instead. Every run appends a unique prefix and removes it afterward; set `HIGHWATER_S3_CHAOS_KEEP=1` to retain the objects for inspection.
