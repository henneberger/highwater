# Future work

Durability and distribution now take precedence over adding operators. Keep this order:

1. Replace the filesystem WAL adapter with object-store conditional writes, a CAS manifest, leader epochs, and fencing.
2. Upload only changed RocksDB checkpoint files; full checkpoints already bound WAL replay.
3. Move the local partition command loops into remotely placeable shard owners while preserving their durable epochs.
4. Move invocation transport from HTTP/JSON to the versioned Protobuf protocol with long polling or streaming RPC.
5. Complete Process build pinning and migration compatibility across rolling deployments.
6. Add durable activation revocation and a configurable maximum total invocation duration; runtime-controlled renewal and owner-epoch fencing are implemented.
7. Add automatic partition assignment, sticky Process caches, and batched durable commits.
8. Materialize large Arrow IPC payloads in object storage and keep references in history.
9. Expand failover, nondeterminism, observability, and history-inspection coverage.

Do not add more language SDKs or a cluster control plane until the first four items are complete.

## Streaming operator follow-ups

The fixed-window, temporal-join, bounded interval-join, and event-time keep-first deduplication operators now have durable state, deterministic Process outputs, and idempotent deployment. The remaining work is:

1. Move keyed operator execution to remote owners; keys and input writes already carry fenced key-group epochs.
2. Replace the filesystem object adapter with conditional object-store writes and incremental checkpoint upload; full RocksDB checkpoints and an atomic manifest are implemented.
3. Add sink-specific adapters; source cursors and the leased transactional outbox provide the current idempotent exactly-once boundary.
4. Add schema-declared composite primary keys; full changelog row kinds and signed operator differences are implemented.
5. Add checkpoint size, state-retention, late-record, watermark-lag, and backpressure metrics.
6. Add credit-based edge backpressure, unaligned checkpoints, and rescaling transfer; native exactly-once edges, aligned remote-owner barriers, and retractable interval-join arrangements are implemented.
7. Move keyed process mailboxes and concurrency permits with their key groups during rescaling; local keyed isolation, capacity backpressure, and completeness gates are implemented.
