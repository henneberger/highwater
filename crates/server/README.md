# Server architecture

The server is a library with a thin binary entrypoint. Dependencies flow from transport and runtime orchestration toward deterministic engines, transactional state, and durable storage.

- `runtime`: configuration, dependency construction, routes, and serving.
- `workflow`, `process`, and `stream_api`: protocol-specific commands and HTTP adapters.
- `operators` and `stream_engine`: deterministic streaming transitions, frontiers, windows, and operator edges.
- `maintenance`: recovery, leases, checkpoints, and outbox maintenance.
- `state` and `storage`: atomic transactions, RocksDB materialization, sharded object WALs, and checkpoints.
- `model`, `keyspace`, and `streaming`: persisted contracts, key encoding, and event-time value types.

Durable mutations must pass through `Transaction` and `DurableStore`; API modules should not write RocksDB or object storage directly.
