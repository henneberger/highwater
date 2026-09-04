# Reliability hardening — 2026-09-04

This change addresses failures found during worker lifecycle review and local
MinIO crash testing. It does not establish production reliability for every
distributed failure history.

A subsequent algorithm review confirmed and repaired a journal ancestor-deletion
race after an ambiguous successful write. The deterministic reproducer and seeded
reference-log simulation are described in [Journal simulation](JOURNAL_SIMULATION.md).

## Repairs

- Process lease renewal now ends on cancellation or errors during execution,
  including errors before completion requests are constructed. Previously, the
  cleanup covered only completion delivery, leaving orphan renewal tasks capable
  of keeping abandoned work leased indefinitely. An HTTP request already running
  in a thread can still finish; cancellation prevents subsequent renewals.
- Workers retry connection errors and HTTP 408, 429, 500, 502, 503, and 504 with
  exponential delays capped at five seconds. A successful iteration resets the
  delay. Other errors propagate. Concurrent execution lanes settle before the next
  iteration, and non-JSON proxy errors retain their HTTP status.
- Output promotion, leasing, and acknowledgement rebuild their transactions from
  synchronized state after a conditional journal append conflict, for at most eight
  attempts. Lease and acknowledgement predicates are rechecked on every attempt;
  responses come only from the successful attempt. This repairs a reproduced
  two-node race where sink polling returned HTTP 400 after an S3 precondition
  failure. This retry helper is restricted to replayable output operations.
- The new reliability workflow runs Rust and Python checks on pull requests and
  main-branch pushes, and adds MinIO crash recovery on manual runs.
  Both validation workflows explicitly build the debug server used by integration
  tests, preventing those tests from silently skipping on fresh CI runners.

## Validation commands

```sh
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo build --locked -p highwater-server
PYTHONPATH=src python3 -m unittest discover -s tests -v
HIGHWATER_S3_CHAOS=1 PYTHONPATH=src python3 -m unittest discover -s tests -p test_s3_chaos.py -v
actionlint .github/workflows/reliability.yml .github/workflows/release-readiness.yml
```

The worker regressions cover cancellation, execution errors, backoff growth and
reset, authentication failures, concurrent lane cleanup, and non-JSON HTTP errors.
The chaos suite covers owner death, fencing, standby checkpoint creation, and
restoration after deletion of the test node's local state.

Final local results: 28 Rust tests passed; 73 Python tests ran with 71 passing
and the two opt-in chaos tests skipped. Both chaos tests then passed in two
consecutive separate MinIO runs after the output repair. Rust formatting,
Clippy, workflow validation, and diff whitespace checks passed.

## Remaining evidence gaps

Real AWS S3, prolonged network partitions, clock skew, rolling upgrades, and
distributed event-time recovery still need dedicated validation. Output markers
and journal history retain data conservatively; bounded garbage collection remains
future work. CI automation takes effect when these files reach GitHub; branch
protection and deployed services have not been changed.
