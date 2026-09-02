from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .model import ChangeKind, StreamRecord
from .streaming import _versioned_runtime


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
        return None if selected.kind == ChangeKind.DELETE else selected.value

    return resolve


async def _run_definition(
    definition: type[Any],
    records: list[StreamRecord],
    versioned_histories: dict[str, list[StreamRecord]],
) -> list[tuple[str, str, Any, Any]]:
    run = getattr(definition, "__highwater_process_run__", None)
    if run is None:
        raise TypeError(f"{definition.__name__} is missing @streaming.process")
    state_version = getattr(definition, "__highwater_state_version__")
    build_id = getattr(definition, "__highwater_build_id__")
    states: dict[str, Any] = {}
    results = []
    resolver = _lookup_resolver(versioned_histories)
    for record in sorted(records, key=lambda value: value.sequence):
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
            "state_version": state_version if key in states else None,
            "target_state_version": state_version,
            "build_id": build_id,
        }
        with _versioned_runtime(resolver):
            transition = await run(definition(), envelope)
        state = transition["state"]
        states[key] = state
        event_id = record.event_id or f"{record.partition}:{record.offset}"
        results.append((event_id, key, state, transition.get("emit")))
    return results


async def compare_process_builds(
    baseline: type[Any],
    candidate: type[Any],
    records: list[StreamRecord],
    *,
    versioned_histories: dict[str, list[StreamRecord]] | None = None,
) -> ReplayComparison:
    histories = versioned_histories or {}
    baseline_results = await _run_definition(baseline, records, histories)
    candidate_results = await _run_definition(candidate, records, histories)
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
    )
