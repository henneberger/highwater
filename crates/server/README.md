# Server architecture

The server is a library with a thin binary entrypoint. Dependencies flow from transport and runtime orchestration toward deterministic engines, transactional state, and durable storage.

- `runtime`: configuration, dependency construction, routes, and serving.
- `workflow`, `process`, and `stream_api`: protocol-specific commands and HTTP adapters.
- `operators` and `stream_engine`: deterministic streaming transitions, frontiers, windows, and operator edges.
- `maintenance`: recovery, leases, checkpoints, and outbox maintenance.
- `state`, `storage`, and `journal`: atomic transactions, RocksDB materialization, conditional partition journals, and checkpoints.
- `model`, `keyspace`, and `streaming`: persisted contracts, key encoding, and event-time value types.

Durable mutations must pass through `Transaction` and `DurableStore`; API modules should not write RocksDB or object storage directly.

Clustered instances use `--journal s3://bucket/prefix` for the authoritative journal, `--process-partitions` for desired ownership, `--advertise-endpoint` for owner routing, and a shared `--cluster-token-file`. A data-only instance adds `--data-plane-only`. `--execution-listen` separates the capability-protected worker API from public ingestion and administration; production sandbox networks should reach only that listener.

Public deployments add `--api-token-file`. This requires a separate execution listener so a bearer credential protects every public API while worker polling remains private.

The local filesystem journal remains the development default. It is not a multi-host coordination mechanism.
