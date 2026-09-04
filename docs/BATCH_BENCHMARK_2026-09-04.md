# Batched compute benchmark — 2026-09-04

Measured on an Apple M3 Pro with 18 GiB RAM, using the current working tree,
including the reliability fixes. Release-mode native server, host Python workers,
and local durable storage; benchmark configurations ran sequentially. This is a
shared development machine, not an isolated production capacity test.

The shopping handler maintains session history, calculates a recommendation score,
and emits output every fifth view. It accepts up to 1,024 events per handler call
with a 2 ms batch delay. Transport batches contain 10,000 events; activation batches
allow 12,000. These limits do not establish the actual average batch fill.

## Results

All short runs submitted 100,000 events across 20,000 session keys.

| Configuration | Runs | Median completed events/s | Range |
| --- | ---: | ---: | ---: |
| Documented baseline: 8 workers / 20 partitions | 3 | 35,240 | 34,485–36,784 |
| Sizing sweep: 8 workers / 8 partitions | 2 | 38,797 | 37,230–40,364 |
| Sizing sweep: 8 workers / 16 partitions | 2 | 30,712 | 19,754–41,670 |
| Sizing sweep: 4 workers / 8 partitions | 2 | 30,892 | 27,264–34,519 |
| Sizing sweep: 10 workers / 10 partitions | 2 | 37,967 | 35,166–40,767 |
| Independent repeat: 8 workers / 8 partitions | 3 | 38,094 | 26,484–38,846 |

A larger run with the same 8-worker / 8-partition configuration completed 500,000
events across 20,000 keys in 25.375 seconds: **19,705 completions/s**, with zero
failed or quarantined events. Admission took 6.276 seconds (79,675 events/s).
This workload has 25 views per session and 100,000 expected emissions, compared
with five views and 20,000 expected emissions in a short run. Completion counts
were verified; emitted payloads and final per-key state were not independently
compared against a reference implementation.

The larger workload's lower rate warrants profiling retained-history scans,
output promotion, checkpoint work, and batch occupancy. These are investigation
targets, not established causes. Reproduce by changing `--events` to `500000` and
`--runs` to `1` in the command below.

The documented 50,000 completed events/s acceptance threshold failed. The historical
54,452 median was not reproduced. Eight workers / eight partitions is a useful
starting configuration on this machine, but the variation is too large to claim a
stable throughput floor. These runs do not isolate the cause of the difference
from the historical benchmark.

The benchmark now checks the final successful completion count and rejects failed
or quarantined events after stopping the timer. Every sizing and repeat run passed
that check. The initial baseline used the previous queue-drain-only check.

## Reproduce

```sh
cargo build --release --locked -p highwater-server
PYTHONPATH=src python3 benchmarks/netherite_partition_throughput.py \
  --events 100000 --active-keys 20000 --runs 3 \
  --partitions 8 --execution-instances 8 --worker-startup-delay 1 \
  --handler batch --workload shopping --minimum-throughput 50000
```

This command intentionally retains the existing acceptance threshold; it fails on
the measurements above. Timing includes ingestion through durable completion, but
excludes setup and the final correctness query. External sink delivery, external
model inference, remote S3, and container overhead are not measured.
