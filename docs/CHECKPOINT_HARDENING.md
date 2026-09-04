# Checkpoint publication and retention hardening

On 2026-09-04, deterministic tests exposed three checkpoint problems. Each new
test was observed failing against the previous implementation before its repair.

## Remote acknowledgement identity

The publication error path previously treated the same checkpoint ID as proof
that its publication succeeded. A competing publisher using that ID with different
contents could therefore cause a rejected publication to be acknowledged.

Readback now compares the complete remote checkpoint: manifest, per-shard vector,
and sorted file paths, sizes, and digests. The race test exercises publication
paused before CAS and successful publication with a lost response paused before
readback, both with distinct and reused IDs. The winner's bytes must restore and
its vector must not regress. A later checkpoint with a larger total position but
a smaller position on an existing partition is rejected.

## Local publication ordering

The local path had no monotonicity check or publication lock. Publishing position
2 could remove its covered WAL, after which a delayed publication of position 1
replaced the recovery pointer. Removing the local database and restoring then
returned counter value 1 instead of acknowledged value 2.

Local publication and cleanup now share a mutex. Before publication, the new
vector must cover every partition in the current manifest and its snapshot must
exist. The old-publication test now rejects the regression and restores value 2.

## Active snapshot retention

Cleanup previously kept the two highest-named snapshot directories, regardless of
which snapshot the recovery pointer referenced. Preparing snapshots 1, 2, and 3,
then publishing snapshot 1, deleted snapshot 1 itself.

Cleanup now considers only snapshots with a strictly smaller sequence than the
published cut. It retains one older snapshot and preserves the current snapshot,
equal cuts, and newer prepared snapshots. The regression test restores snapshot 1
plus journal entries 2 and 3 and verifies the final acknowledged state.

This intentionally retains more local snapshots when preparation outpaces
publication. It does not implement bounded garbage collection for remote journal
objects, output markers, or indefinitely abandoned prepared snapshots. Retaining
an older snapshot also does not promise that all history needed to recover from
that older snapshot remains available; the authoritative pointer defines recovery.

## Reproduce

```sh
cargo test -p highwater-server checkpoint -- --nocapture
cargo test --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
HIGHWATER_S3_CHAOS=1 PYTHONPATH=src python3 -m unittest discover -s tests -p test_s3_chaos.py -v
```

The remote race test uses the real conditional-publication implementation with an
in-memory object store and controlled pause points. Local tests use RocksDB,
actual checkpoints, WAL cleanup, deletion of the temporary local database, and
restore. Hooks are compiled only for tests. Extended CI checks run manually;
there is no nightly schedule in the reliability workflow.

Final local results: all 34 Rust workspace tests passed, including all eight
checkpoint tests; both MinIO crash/recovery scenarios passed. Clippy, formatting,
workflow syntax, and diff whitespace checks passed.
