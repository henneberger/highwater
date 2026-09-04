# Live-owner network isolation

The MinIO isolation drill disconnects only core A's object-store TCP connections.
It does not kill or suspend A, change host firewall rules, or interrupt core B's
storage access. A local proxy closes established connections and rejects new ones
until the test heals the link. A unit test verifies both connection cutoff and
reconnection behavior.

The drill executes this history:

1. A owns both partitions and grants an activation whose completion is held back.
2. A's storage link is cut. A stays alive, and publishing continues through B.
3. After owner-lease expiry, B takes ownership and its real Python worker executes
   the retained and newly admitted events.
4. A receives a stale completion while isolated. It must not acknowledge success;
   a lost response remains uncertain until the final state check.
5. A's storage link is restored. A synchronizes the higher owner epochs. Both A
   and B reject completion and renewal using the old grant.
6. The producer retries earlier event IDs and publishes more events through A,
   which must route them to B. All 65 unique events finish without failure, with
   per-key totals 17, 16, 16, and 16. Both core processes and the worker survive.

## Reproduced availability failure

Before the fix, a storage connection reset on A was returned as HTTP 400 through
B's forwarding path. The SDK interpreted it as a permanent rejection and stopped
publishing instead of retrying across takeover. The new test reproduced this
failure before the code change.

The journal service now passes typed errors through its in-process response
channels. Process-task responses use a cloneable shared error that preserves the
source chain when a group commit reports one failure to multiple callers. The API
recognizes object-store errors through that chain and responds with HTTP 503.
Existing SDK/worker retry behavior then handles the uncertain response. Application
validation errors remain HTTP 400; a separate unit test protects that distinction.
Checkpoint and append error contexts retain the original storage error as well.

This does not make progress possible without durable storage. Persistent storage
permission, missing-object, or configuration errors still require operator action;
retryability does not imply that every failure heals automatically.

## Reproduce

```sh
HIGHWATER_S3_CHAOS=1 PYTHONPATH=src python3 -m unittest discover \
  -s tests -p test_s3_chaos.py -k isolated_live -v
```

The drill is local-MinIO-only and skips when `HIGHWATER_S3_CHAOS_URI` selects an
external store. It uses connection resets, not packet loss or a one-way blackhole.
It does not isolate all cluster links, simulate clock skew, or prove safety across
arbitrary network schedules. Existing deterministic journal tests separately cover
successful writes with lost responses and a successor committed before readback.

No production infrastructure is modified. The drill is opt-in locally and part of
the manual chaos CI step; no nightly schedule is enabled.

## Validation results

Local validation on 2026-09-04 passed:

- All 37 Rust workspace tests, formatting checks, and Clippy.
- Python discovery: 79 tests, with 75 passing and four opt-in chaos tests skipped.
- The separate opt-in MinIO suite: all four chaos tests passed (80.7 seconds),
  including live-owner isolation, two successive core failures, non-owner
  checkpoint publication, and stale-work fencing with checkpoint recovery.
- The proxy unit test also passed through the module-based test invocation used
  by release validation.
