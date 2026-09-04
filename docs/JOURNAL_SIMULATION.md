# Deterministic journal recovery validation

## Confirmed failure and repair

On 2026-09-04, the new deterministic test
`ambiguous_response_after_takeover_preserves_committed_ancestor` reproduced this
history against the real `ConditionalJournal::append` implementation:

1. Writer A publishes record A through the conditional head update.
2. Its successful response is lost; the test pauses before error readback.
3. Writer B observes A and successfully publishes record B with A as its parent.
4. Writer A resumes. Its readback sees B rather than A.
5. The old error path deletes A, making B's acknowledged history unrecoverable.

Before the fix, recovery failed with an object-not-found error. The same test
passes after removing deletion from the uncertain append path. The caller still
receives an error if readback cannot establish that its record is the current
head. It must synchronize and apply normal idempotency/fencing rules before
retrying; the implementation does not return a successor cursor whose payload the
caller has not applied.

Failed append attempts now retain their immutable objects. Some are unreachable
orphans. Reclamation requires a separate reachability and checkpoint-retention
protocol; observing a different head is insufficient evidence for deletion.

## Executable reference checks

`seeded_journal_histories_match_reference_log` runs three simulated writers
against the actual journal and an in-memory conditional object store. Each writer
has an independent cursor. A separate reference log records expected accepted
payloads and owner epochs. After every scheduled action, recovery must return
exactly that log, including contiguous positions and the expected head position.

Generated actions include synchronization, stale-cursor writes, owner generation
changes, invalid epoch advances, crashes between immutable-record creation and
head publication, successful writes with lost responses, and a successor committed
before ambiguous-response readback. Restart checks discard a simulated local
cursor and compare a reference checkpoint prefix plus actual recovered tail with
the full reference log.

| Algorithm concept | Implementation exercised | Check |
| --- | --- | --- |
| Append linearization | Conditional head `put_opts` | Only writes allowed by the reference head enter the log |
| Owner generation | `append` epoch validation and CAS | Invalid advances and stale cursors cannot alter committed history |
| Uncertain outcome | Pause after successful CAS, before error readback | A successor cannot make an ancestor disposable |
| Local state loss | Discard cursor; call `recover` | Exact positions, epochs, and payloads are reconstructed |
| Checkpoint cut | Reference prefix plus `recover(after_position)` | Every generated cut reconstructs the reference log |

These checks cover the journal layer. The reference checkpoint prefix is not a
real RocksDB checkpoint; separate existing tests exercise checkpoint publication
and restore. This is not a refinement proof for the TLA+ ownership model or a
simulation of the complete process, watermark, sink, or deployment lifecycle.

## Run and replay

```sh
# Default: 128 histories, 64 scheduled actions each.
cargo test -p highwater-server seeded_journal_histories_match_reference_log

# Extended manual CI run: 4,096 histories, 262,144 actions.
HIGHWATER_JOURNAL_SEED_COUNT=4096 cargo test -p highwater-server \
  seeded_journal_histories_match_reference_log -- --nocapture

# Replay exactly one seed reported by a failure.
HIGHWATER_JOURNAL_SEED=42 cargo test -p highwater-server \
  seeded_journal_histories_match_reference_log -- --nocapture
```

Failures print the seed and action trace to the test log. Replay is deterministic
at the scheduled operation boundaries; object names contain random UUIDs but do
not choose the schedule. There is no automatic trace shrinking yet. All injected
hooks are compiled only for tests. The concrete regression was observed failing
before the deletion repair and passing afterward.

Local validation on 2026-09-04: all 30 Rust workspace tests passed; the extended
4,096-seed run passed all 262,144 scheduled actions in 26.69 seconds; replay of seed
42 passed; both MinIO crash/checkpoint recovery scenarios passed. Clippy,
formatting, workflow syntax, and diff whitespace checks passed.

The next layer is now covered by [process simulation](PROCESS_SIMULATION.md),
including admissions, deduplication, leases, outcomes, pending output, and progress
under a controlled test clock. [Checkpoint hardening](CHECKPOINT_HARDENING.md)
adds competing-publisher and local retention regressions. Storage read failures,
bounded garbage collection, distributed event time, and rolling upgrades remain
separate work.
