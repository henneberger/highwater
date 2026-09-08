from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any

from .model import ChangeKind, StreamRecord
from .streaming import _versioned_runtime


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class ReplayManifest:
    """Portable replay inputs. The digest detects modification, not code identity."""

    payload: str
    sha256: str

    def __post_init__(self) -> None:
        if hashlib.sha256(self.payload.encode()).hexdigest() != self.sha256:
            raise ValueError("replay manifest checksum mismatch")
        data = json.loads(self.payload)
        if data.get("version") != 1:
            raise ValueError("unsupported replay manifest version")
        if _canonical(data) != self.payload:
            raise ValueError("replay manifest payload must be canonical JSON")

    @classmethod
    def capture(
        cls,
        baseline: type[Any],
        candidate: type[Any],
        records: list[StreamRecord],
        *,
        versioned_histories: dict[str, list[StreamRecord]] | None = None,
        initial_states: dict[str, Any] | None = None,
        initial_state_version: int | None = None,
    ) -> ReplayManifest:
        if initial_states and (
            type(initial_state_version) is not int or initial_state_version <= 0
        ):
            raise ValueError("initial states require a positive initial_state_version")
        if any(not isinstance(key, str) or not key for key in (initial_states or {})):
            raise ValueError("initial state keys must be nonempty strings")
        builds = []
        dependencies = set()
        for definition in (baseline, candidate):
            if getattr(definition, "__highwater_process_run__", None) is None:
                raise TypeError(f"{definition.__name__} is missing @streaming.process")
            builds.append({
                "build_id": definition.__highwater_build_id__,
                "state_version": definition.__highwater_state_version__,
            })
            dependencies.update(definition.__highwater_versioned_streams__)
        histories = versioned_histories or {}
        missing = dependencies - histories.keys()
        if missing:
            raise ValueError(f"replay requires captured histories for: {sorted(missing)}")
        ordered = sorted(records, key=lambda record: record.sequence)
        if len({record.sequence for record in ordered}) != len(ordered):
            raise ValueError("replay input sequence numbers must be unique")
        if len({record.stream for record in ordered}) > 1:
            raise ValueError("replay input must belong to one stream")
        payload = _canonical({
            "version": 1,
            "builds": builds,
            "records": [asdict(record) for record in ordered],
            "versioned_histories": {
                name: [asdict(record) for record in sorted(history, key=lambda r: r.sequence)]
                for name, history in histories.items()
            },
            "initial_states": initial_states or {},
            "initial_state_version": initial_state_version,
        })
        return cls(payload, hashlib.sha256(payload.encode()).hexdigest())

    def to_json(self) -> str:
        return _canonical({"payload": json.loads(self.payload), "sha256": self.sha256})

    @classmethod
    def from_json(cls, value: str) -> ReplayManifest:
        document = json.loads(value)
        return cls(_canonical(document["payload"]), document["sha256"])

    def inputs(self) -> dict[str, Any]:
        """Return an independent copy; mutations cannot change this manifest."""
        return json.loads(self.payload)


@dataclass(frozen=True)
class ReplayDifference:
    event_id: str
    key: str
    baseline_state: Any
    candidate_state: Any
    baseline_output: Any
    candidate_output: Any


@dataclass(frozen=True)
class ReplayComparison:
    events: int
    matching_events: int
    differences: tuple[ReplayDifference, ...]
    manifest: ReplayManifest | None = None

    @property
    def matches(self) -> bool:
        return not self.differences


def _lookup_resolver(
    histories: dict[str, list[StreamRecord]],
):
    def resolve(stream: str, key: str, as_of: float) -> Any:
        eligible = [
            record
            for record in histories.get(stream, [])
            if record.key == key and record.event_time <= as_of
        ]
        if not eligible:
            return None
        selected = max(eligible, key=lambda record: (record.event_time, record.sequence))
        return None if selected.kind == ChangeKind.DELETE else deepcopy(selected.value)

    return resolve


async def _run_definition(
    definition: type[Any],
    records: list[StreamRecord],
    versioned_histories: dict[str, list[StreamRecord]],
    initial_states: dict[str, Any],
    initial_state_version: int | None,
) -> list[tuple[str, str, Any, Any]]:
    run = getattr(definition, "__highwater_process_run__", None)
    if run is None:
        raise TypeError(f"{definition.__name__} is missing @streaming.process")
    state_version = getattr(definition, "__highwater_state_version__")
    build_id = getattr(definition, "__highwater_build_id__")
    states = deepcopy(initial_states)
    versions = {key: initial_state_version for key in states}
    results = []
    resolver = _lookup_resolver(versioned_histories)
    for record in deepcopy(sorted(records, key=lambda value: value.sequence)):
        key = record.key
        if key is None or not key:
            raise ValueError("Process replay requires a key on every source record")
        envelope = {
            "process_id": "replay",
            "key": key,
            "event_time": record.event_time,
            "record": {
                "value": record.value,
                "kind": record.kind,
                "event_id": record.event_id,
            },
            "state": states.get(key),
            "state_version": versions.get(key),
            "target_state_version": state_version,
            "build_id": build_id,
        }
        with _versioned_runtime(resolver):
            transition = await run(definition(), envelope)
        state = transition["state"]
        states[key] = deepcopy(state)
        versions[key] = state_version
        event_id = record.event_id or f"{record.partition}:{record.offset}"
        results.append(deepcopy((event_id, key, state, transition.get("emit"))))
    return results


async def compare_process_builds(
    baseline: type[Any],
    candidate: type[Any],
    records: list[StreamRecord] | None = None,
    *,
    versioned_histories: dict[str, list[StreamRecord]] | None = None,
    initial_states: dict[str, Any] | None = None,
    initial_state_version: int | None = None,
    manifest: ReplayManifest | None = None,
) -> ReplayComparison:
    if manifest is not None:
        if any(value is not None for value in (
            records, versioned_histories, initial_states, initial_state_version,
        )):
            raise ValueError("manifest cannot be combined with new replay inputs")
    else:
        if records is None:
            raise ValueError("records or manifest is required")
        manifest = ReplayManifest.capture(
            baseline, candidate, records, versioned_histories=versioned_histories,
            initial_states=initial_states, initial_state_version=initial_state_version,
        )
    data = manifest.inputs()
    for definition, pinned in zip((baseline, candidate), data["builds"], strict=True):
        if (getattr(definition, "__highwater_build_id__", None) != pinned["build_id"]
                or getattr(definition, "__highwater_state_version__", None) != pinned["state_version"]):
            raise ValueError("replay build ID or state version differs from manifest")
        missing = set(definition.__highwater_versioned_streams__) - data["versioned_histories"].keys()
        if missing:
            raise ValueError(f"replay manifest is missing histories for: {sorted(missing)}")
    records = [StreamRecord(**value) for value in data["records"]]
    histories = {
        name: [StreamRecord(**value) for value in history]
        for name, history in data["versioned_histories"].items()
    }
    baseline_results = await _run_definition(
        baseline, records, histories, data["initial_states"], data["initial_state_version"],
    )
    candidate_results = await _run_definition(
        candidate, records, histories, data["initial_states"], data["initial_state_version"],
    )
    if len(baseline_results) != len(candidate_results):
        raise RuntimeError("replay builds produced different result cardinality")
    differences = []
    for before, after in zip(baseline_results, candidate_results, strict=True):
        event_id, key, baseline_state, baseline_output = before
        candidate_event_id, candidate_key, candidate_state, candidate_output = after
        if (event_id, key) != (candidate_event_id, candidate_key):
            raise RuntimeError("replay builds processed events in different order")
        if baseline_state != candidate_state or baseline_output != candidate_output:
            differences.append(ReplayDifference(
                event_id=event_id,
                key=key,
                baseline_state=baseline_state,
                candidate_state=candidate_state,
                baseline_output=baseline_output,
                candidate_output=candidate_output,
            ))
    return ReplayComparison(
        events=len(records),
        matching_events=len(records) - len(differences),
        differences=tuple(differences),
        manifest=manifest,
    )
