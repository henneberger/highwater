# High availability for distributed process execution

The reference topology now provides two candidate cores per process partition,
two worker pools, and a public Service routing to both cores. One durable journal
head and owner epoch remain authoritative for each partition. A standby may take
over after lease expiry; it cannot issue work while another runtime owns the
partition. Handler invocation and sink delivery remain at least once.

Ingestion into a surviving core retries with stable event identities when the
current owner is unreachable, moving, or loses a conditional append race. Cluster
forwarding has a five-second request timeout. The Python SDK treats HTTP 429,
502, 503, and 504 as retryable admission backpressure, including non-JSON proxy
errors, preserving the event body across uncertain responses. Non-idempotent API
operations are not automatically replayed by the generic request method.

Workers stay attached to a particular core endpoint so completions and renewals
return to the grant issuer. Workers reconnect after that endpoint returns; the
surviving core's existing pool executes taken-over partitions. The deployment
retains a minimum of one worker in each pool and uses separate-host placement.
Initial ownership need not be evenly balanced. Automatic load-aware placement
remains separate work.

## Failure drill

```sh
HIGHWATER_S3_CHAOS=1 PYTHONPATH=src python3 -m unittest discover \
  -s tests -p test_s3_chaos.py -k live_workers -v
```

The drill starts two independent core processes sharing MinIO, two real Python
workers, and a producer publishing through the non-owner core. It kills the owner
during admission, waits for takeover, starts a replacement core with empty local
storage, then kills the second owner during another publication phase. Neither
worker is manually restarted. All 96 unique events must complete without failure,
and all four keys must end at total 24. The test checks increasing owner epochs
and the continued survival of both worker processes. Separate existing tests
exercise delayed stale completions and acknowledged output recovery.

## Deployment boundary

- Use a durable, highly available object store with the required conditional-write
  semantics. Losing object-store access prevents durable progress; running two
  cores does not remove that dependency.
- Put the public API behind a healthy-endpoint-aware gateway/Service. The SDK uses
  one configured endpoint; it does not discover replacement hosts itself.
- Expect a failover pause for owner lease expiry, replay, and retry backoff. The
  manifest uses the engine's configured/default owner lease, not the shorter
  test lease. No zero-downtime or fixed recovery-time claim is established here.
- Roll only one core Deployment at a time. Keep enough capacity on the surviving
  hosts and use unique runtime/node identities. Avoid simultaneous core outages.
- The validated path is direct keyed Process execution. Distributed workflows,
  stream-fed event-time operators, network partitions, zone loss, and a real AWS
  deployment require further failure testing.

The local drill uses MinIO and OS processes, not a deployed Kubernetes cluster.
No production infrastructure has been changed. Extended CI tests are manual-only;
there is no nightly schedule.

[Live-owner network isolation](NETWORK_PARTITION_VALIDATION.md) adds a surviving
but storage-isolated old owner, takeover, reconnection, stale-grant rejection, and
producer retry checks. It also documents the storage-error classification defect
that the isolation drill reproduced and repaired.

Validation on 2026-09-04: 35 Rust workspace tests passed; the Python suite ran 77
tests with 74 passing and three opt-in chaos tests skipped. All three chaos tests
then passed separately, including a second successful live-worker failover run.
Clippy, Rust formatting, diff checks, manifest renderer tests, and Kubernetes
client-side dry-run validation passed. This does not establish a production
recovery-time SLA or validate cloud-specific networking and storage behavior.
