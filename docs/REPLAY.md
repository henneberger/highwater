# Reproducible build comparison

`compare_process_builds` executes two decorated Process classes against the same
captured inputs and reports state or output differences after each event. Every
comparison includes a `ReplayManifest` containing the ordered records, versioned
reference histories, initial keyed states and their schema version, and both
build IDs and target state versions. By default, each key starts from its class
constructor defaults.

```python
from pathlib import Path
from highwater import ReplayManifest, compare_process_builds

comparison = await client.compare_builds(
    "shopping-assistant",
    baseline=ShoppingAssistantV1,
    candidate=ShoppingAssistantV2,
)
Path("comparison.json").write_text(comparison.manifest.to_json())

# This run uses the saved inputs and requires no connection to Highwater.
manifest = ReplayManifest.from_json(Path("comparison.json").read_text())
repeated = await compare_process_builds(
    ShoppingAssistantV1, ShoppingAssistantV2, manifest=manifest,
)
```

The canonical JSON payload has a SHA-256 checksum, verified on loading. Reuse
rejects different build IDs or target state versions and cannot be combined with
replacement input arguments. Keep the matching code and dependency versions;
build IDs are labels and the checksum does not authenticate executable code.

For a suffix of a stream, supply `initial_states={key: state}` and a positive
`initial_state_version` when creating the comparison or using
`ReplayManifest.capture(...)`. The state must precede the first supplied record.
Each build receives an independent copy and runs its declared migrations. A
manifest includes complete record values, so store it with the same access
controls as its source data.

Versioned lookups use the captured history at the event's original event time.
Every declared reference stream must have a captured history; an empty list
explicitly represents no versions. State and outputs are copied after each
transition so later mutations cannot change earlier comparison results.

## Boundaries

The client captures currently retained input and reference histories through
separate reads. This records exactly what the comparison used; it does not
establish a coherent snapshot across live streams or recover discarded history.
The helper replays supplied input by sequence, without simulating admission,
retries, watermark waiting, or output delivery. Use the local helper with an
explicitly selected input suffix when supplying nonempty initial state; the
client otherwise reads all retained input.

Application code executes locally. External responses, clocks, randomness, and
library versions are not recorded or intercepted. Supply recorded responses or
an evaluation implementation for external calls; such calls can still produce
side effects. The helper itself does not write state or outputs to Highwater.
This is a reproducible input bundle for deterministic handlers, not a full
system execution branch.
