# Performance

Highwater measures completed durable transitions. The primary benchmark starts with empty Process state, admits 100,000 product views across 20,000 shopping sessions and 10,000 products, and waits for every completion. Each transition updates bounded session history, calculates a recommendation score, and emits durable output every fifth view.

## Measured development-machine runs

| Worker boundary | Completed events/s |
| --- | ---: |
| 8 host workers, median of three runs | 54,452 |
| 10 hardened container workers | 45,782 |
| 10 hardened container workers, minimal counter | 65,974 |

The container profile runs as UID 65532 with a read-only root filesystem, dropped capabilities, `no-new-privileges`, and CPU, memory, and process limits. These results describe one development machine. Cluster storage and networking need measurements in the release environment.

## Reproduce

```bash
cargo build --release --package highwater-server
docker build -f Dockerfile.worker -t highwater-worker:release-test .

PYTHONPATH=src python3 benchmarks/netherite_partition_throughput.py \
  --events 100000 --active-keys 20000 --runs 3 \
  --partitions 20 --execution-instances 8 --worker-startup-delay 1 \
  --workload shopping --minimum-throughput 50000

PYTHONPATH=src python3 benchmarks/netherite_partition_throughput.py \
  --events 100000 --active-keys 20000 --runs 1 \
  --partitions 20 --execution-instances 10 \
  --workload shopping --worker-runtime docker \
  --worker-image highwater-worker:release-test \
  --worker-startup-delay 2 --minimum-throughput 40000
```

The harness includes HTTP ingestion, key routing, serialization, authoritative admission and completion appends, Python execution, result transport, state transitions, and output commits. The shopping workload does not call an external model or service. Versioned lookups, event-time behavior, replay comparison, and owner recovery have separate correctness and failure tests.

One noncommutative key remains serial. Production capacity testing also needs the target object store, network, sandbox runtime, key distribution, state size, and downstream calls.
