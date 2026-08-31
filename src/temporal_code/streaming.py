from __future__ import annotations

import hashlib
import inspect
import math
import marshal
from dataclasses import asdict, dataclass, fields, is_dataclass
from typing import Any, Generic, TypeVar, get_args, get_origin, get_type_hints, overload

from .model import EventTimeGate

S = TypeVar("S")
O = TypeVar("O")
T = TypeVar("T", bound=type)


def _json_value(value: Any) -> Any:
    return asdict(value) if is_dataclass(value) else value


def _build_id(target: type, state_version: int, methods: list[Any]) -> str:
    digest = hashlib.sha256()
    digest.update(f"{target.__module__}.{target.__qualname__}:{state_version}".encode())
    for method in methods:
        digest.update(method.__name__.encode())
        digest.update(marshal.dumps(method.__code__))
    return digest.hexdigest()


@dataclass(frozen=True)
class ProcessContext(Generic[S]):
    process_id: str
    key: str
    event_time: float
    state: S | None
    record: dict[str, Any]

    def state_or(self, default: S) -> S:
        return default if self.state is None else self.state


@dataclass(frozen=True)
class Transition(Generic[S, O]):
    state: S
    emit: O | None = None

    def _wire(self) -> dict[str, Any]:
        return {
            "__temporal_code_transition__": True,
            "state": _json_value(self.state),
            "emit": _json_value(self.emit),
        }


class _StreamingAnnotations:
    immediate = EventTimeGate.IMMEDIATE
    complete = EventTimeGate.COMPLETE

    @overload
    def process(self, cls: T) -> T: ...

    @overload
    def process(
        self,
        *,
        name: str | None = None,
        key: str | None = None,
        event_time: str | None = None,
        wait_until: EventTimeGate = EventTimeGate.IMMEDIATE,
        state_version: int = 1,
        build_id: str | None = None,
    ) -> Any: ...

    def process(
        self,
        cls: T | None = None,
        *,
        name: str | None = None,
        key: str | None = None,
        event_time: str | None = None,
        wait_until: EventTimeGate = EventTimeGate.IMMEDIATE,
        state_version: int = 1,
        build_id: str | None = None,
    ):
        def decorate(target: T) -> T:
            if state_version <= 0:
                raise ValueError("process state_version must be positive")
            if build_id is not None and not build_id.strip():
                raise ValueError("process build_id must not be empty")
            event_handlers = [
                (attr, member)
                for attr, member in vars(target).items()
                if getattr(member, "__temporal_code_kind__", None) == "process_event"
            ]
            batch_handlers = [
                (attr, member)
                for attr, member in vars(target).items()
                if getattr(member, "__temporal_code_kind__", None) == "process_batch"
            ]
            if len(event_handlers) + len(batch_handlers) != 1:
                raise TypeError(
                    f"{target.__name__} must have exactly one @streaming.event or @streaming.batch method"
                )
            handler_name, handler = (event_handlers or batch_handlers)[0]
            is_batch = bool(batch_handlers)
            parameter = list(inspect.signature(handler).parameters.values())[1]
            annotation = get_type_hints(handler).get(parameter.name)
            event_type = annotation
            if is_batch and get_origin(annotation) is list:
                event_type = get_args(annotation)[0]
            migration_methods = [
                (getattr(member, "__temporal_code_migration_from__"), attr, member)
                for attr, member in vars(target).items()
                if getattr(member, "__temporal_code_kind__", None) == "process_migration"
            ]
            migrations = {version: attr for version, attr, _ in migration_methods}
            if len(migrations) != len(migration_methods):
                raise TypeError("process migration versions must be unique")
            if any(version <= 0 or version >= state_version for version in migrations):
                raise ValueError("process migrations must start below the current state version")
            selected_build_id = build_id or _build_id(
                target,
                state_version,
                [handler] + [member for _, _, member in sorted(migration_methods)],
            )

            async def prepare(
                instance: Any,
                envelope: dict[str, Any],
            ) -> tuple[Any, ProcessContext[Any]]:
                state = envelope.get("state")
                stored_version = envelope.get("state_version")
                if state is not None:
                    if stored_version is None:
                        raise TypeError("persisted process state is missing its version")
                    if stored_version > state_version:
                        raise TypeError(
                            f"state version {stored_version} is newer than worker version {state_version}"
                        )
                    while stored_version < state_version:
                        migration_name = migrations.get(stored_version)
                        if migration_name is None:
                            raise TypeError(
                                f"missing process migration from state version {stored_version}"
                            )
                        state = getattr(instance, migration_name)(state)
                        if inspect.isawaitable(state):
                            state = await state
                        state = _json_value(state)
                        stored_version += 1
                if state is not None and is_dataclass(instance):
                    known_fields = {field.name for field in fields(instance)}
                    unknown = set(state) - known_fields
                    if unknown:
                        raise TypeError(f"process state has unknown fields: {sorted(unknown)}")
                    for field_name, value in state.items():
                        setattr(instance, field_name, value)
                context = ProcessContext(
                    process_id=envelope["process_id"],
                    key=envelope["key"],
                    event_time=envelope["event_time"],
                    state=state,
                    record=envelope["record"],
                )
                event = envelope["record"]["value"]
                if event_type is not None and is_dataclass(event_type) and isinstance(event, dict):
                    event = event_type(**event)
                return event, context

            async def batch_run(
                coordinator: Any,
                envelopes: list[dict[str, Any]],
            ) -> list[dict[str, Any]]:
                instances = [target() for _ in envelopes]
                prepared = [
                    await prepare(instance, envelope)
                    for instance, envelope in zip(instances, envelopes, strict=True)
                ]
                events = [event for event, _ in prepared]
                contexts = [context for _, context in prepared]
                bound_handler = getattr(coordinator, handler_name)
                parameters = len(inspect.signature(bound_handler).parameters)
                if parameters not in {1, 2}:
                    raise TypeError(
                        "@streaming.batch accepts events and optional ProcessContext list"
                    )
                results = (
                    bound_handler(events)
                    if parameters == 1
                    else bound_handler(events, contexts)
                )
                if inspect.isawaitable(results):
                    results = await results
                if not isinstance(results, list) or len(results) != len(envelopes):
                    raise TypeError("@streaming.batch must return one result for each event")
                transitions = []
                for result, instance, context in zip(results, instances, contexts, strict=True):
                    if isinstance(result, Transition):
                        transitions.append(result._wire())
                    else:
                        state = asdict(instance) if is_dataclass(instance) else context.state or {}
                        transitions.append(Transition(state=state, emit=result)._wire())
                return transitions

            async def run(instance: Any, envelope: dict[str, Any]) -> Any:
                if is_batch:
                    return (await batch_run(instance, [envelope]))[0]
                event, context = await prepare(instance, envelope)
                bound_handler = getattr(instance, handler_name)
                parameters = len(inspect.signature(bound_handler).parameters)
                if parameters not in {1, 2}:
                    raise TypeError(
                        "@streaming.event accepts event and optional ProcessContext"
                    )
                result = bound_handler(event) if parameters == 1 else bound_handler(event, context)
                if inspect.isawaitable(result):
                    result = await result
                if isinstance(result, Transition):
                    return result._wire()
                if not is_dataclass(instance):
                    raise TypeError(
                        "non-dataclass streaming handlers must return streaming.transition(...)"
                    )
                return Transition(state=asdict(instance), emit=result)._wire()

            setattr(run, "__temporal_code_kind__", "run")
            setattr(run, "__temporal_code_name__", "run")
            selected_name = name or target.__name__
            setattr(target, "__temporal_code_process__", selected_name)
            setattr(target, "__temporal_code_workflow__", selected_name)
            setattr(target, "__temporal_code_process_run__", run)
            setattr(target, "__temporal_code_process_batch_run__", batch_run if is_batch else None)
            setattr(target, "__temporal_code_process_key__", key)
            setattr(target, "__temporal_code_process_event_time__", event_time)
            setattr(target, "__temporal_code_process_gate__", wait_until)
            setattr(target, "__temporal_code_state_version__", state_version)
            setattr(target, "__temporal_code_build_id__", selected_build_id)
            setattr(target, "__temporal_code_migrations_from__", tuple(sorted(migrations)))
            setattr(target, "__temporal_code_batch_max_size__", getattr(handler, "__temporal_code_batch_max_size__", 64))
            setattr(target, "__temporal_code_batch_max_delay__", getattr(handler, "__temporal_code_batch_max_delay__", 0.005))
            return target

        return decorate(cls) if cls is not None else decorate

    def event(self, fn: Any) -> Any:
        setattr(fn, "__temporal_code_kind__", "process_event")
        setattr(fn, "__temporal_code_name__", fn.__name__)
        return fn

    def batch(self, *, max_size: int = 128, max_delay: float = 0.01):
        if not 1 <= max_size <= 1_024:
            raise ValueError("batch max_size must be between 1 and 1024")
        if not math.isfinite(max_delay) or max_delay < 0:
            raise ValueError("batch max_delay must be finite and non-negative")

        def decorate(fn: Any) -> Any:
            setattr(fn, "__temporal_code_kind__", "process_batch")
            setattr(fn, "__temporal_code_name__", fn.__name__)
            setattr(fn, "__temporal_code_batch_max_size__", max_size)
            setattr(fn, "__temporal_code_batch_max_delay__", float(max_delay))
            return fn

        return decorate

    def migrate(self, *, from_version: int):
        if from_version <= 0:
            raise ValueError("migration version must be positive")

        def decorate(fn: Any) -> Any:
            setattr(fn, "__temporal_code_kind__", "process_migration")
            setattr(fn, "__temporal_code_migration_from__", from_version)
            return fn

        return decorate

    @staticmethod
    def transition(state: S, *, emit: O | None = None) -> Transition[S, O]:
        return Transition(state=state, emit=emit)


streaming = _StreamingAnnotations()
