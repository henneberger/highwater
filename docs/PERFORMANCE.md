# Performance

Highwater measures completed durable transitions, not handler calls or in-memory queue operations. The benchmark starts with empty state, admits 100,000 distinct keys, runs the Python application across disjoint partition assignments, and waits until every completion is committed. Both admission and completion acknowledgements follow an authoritative WAL append and sync.

## Partition scaling

The following results were collected on one Apple Silicon development machine with 10 data partitions. Each value is the median of three runs; the minimum shows run-to-run headroom against the 50,000 events-per-second target.

| Execution instances | Median completed events/s | Minimum completed events/s |
| ---: | ---: | ---: |
| 1 | 55,310 | 52,372 |
| 2 | 86,495 | 84,771 |
| 5 | 99,951 | 95,048 |
| 10 | 102,904 | 99,987 |

Five instances using the ordinary per-event handler completed a median 89,029 events/s, with a minimum of 84,200 events/s. The batched handler reduces Python dispatch overhead but does not weaken the per-key transition or durability boundary.

These are single-machine results, not a cluster capacity claim. They demonstrate that disjoint partition assignment distributes application compute and that the durable data path exceeds the current target. Multi-host service ownership, replicated persistence, and automatic placement require separate failure and capacity measurements.

## Reproduce

Build the service once, then run the self-contained harness:

```bash
cargo build --release --bin highwater-server
python3 benchmarks/netherite_partition_throughput.py \
  --server target/release/highwater-server \
  --events 100000 \
  --partitions 10 \
  --execution-instances 5 \
  --handler batch \
  --runs 3
```

Use `--handler event` to measure the ordinary Process callback. The harness creates isolated durable state, starts the service, assigns every data partition to exactly one execution process, reports each run, and removes the temporary state after shutdown.

## Interpret the result

The benchmark includes HTTP ingestion, key routing, serialization, object-WAL append and sync, scheduling, Python execution, result transport, and atomic completion. It excludes replicated storage latency, cross-host network latency, checkpoint upload, external sinks, and application-specific work.

Throughput is expected to scale with independent partitions until storage, the service process, or the host becomes saturated. A single key is intentionally serial. Production capacity planning must also measure p99 completion latency, checkpoint pressure, recovery time, hot-key skew, and performance during owner movement.
