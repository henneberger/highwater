# Future work

Durability and distribution now take precedence over adding operators. Keep this order:

1. Run deterministic S3 fault injection around ambiguous head writes, checkpoint publication, source death, and target activation.
2. Add automatic partition placement and load-aware movement on top of the implemented manual handoff.
3. Move invocation and cluster transport from HTTP/JSON to the versioned Protobuf protocol with streaming RPC.
4. Complete Process build pinning and migration compatibility across rolling deployments.
5. Add a configurable maximum total invocation duration; durable revocation, runtime renewal, and owner-epoch fencing are implemented.
6. Add sticky Process caches and extend group commit to every transition type.
7. Materialize large Arrow IPC payloads in object storage and keep references in history.
8. Expand nondeterminism, observability, and history-inspection coverage.

Do not add more language SDKs or a cluster control plane until the first four items are complete.

## Streaming operator follow-ups

The fixed-window, temporal-join, bounded interval-join, and event-time keep-first deduplication operators now have durable state, deterministic Process outputs, and idempotent deployment. The remaining work is:

1. Move keyed operator execution to remote owners; keys and input writes already carry fenced key-group epochs.
2. Add sink-specific adapters; source cursors and the leased transactional outbox provide the current idempotent exactly-once boundary.
3. Add schema-declared composite primary keys; full changelog row kinds and signed operator differences are implemented.
4. Add checkpoint size, state-retention, late-record, watermark-lag, and backpressure metrics.
5. Add credit-based edge backpressure, unaligned checkpoints, and rescaling transfer; native exactly-once edges, aligned remote-owner barriers, and retractable interval-join arrangements are implemented.
6. Move keyed process mailboxes and concurrency permits with their key groups during rescaling; local keyed isolation, capacity backpressure, and completeness gates are implemented.
