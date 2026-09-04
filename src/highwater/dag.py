from __future__ import annotations

from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Any

from .model import (
    Comparison,
    DeduplicateSpec,
    EventTimeGate,
    FilterSpec,
    IntervalJoinType,
    IntervalJoinSpec,
    ProcessOptions,
    ProcessSpec,
    StreamOptions,
    TemporalJoinType,
    TemporalJoinSpec,
    WindowAggregateSpec,
    WindowAggregation,
)

OperatorSpec = (
    ProcessSpec
    | FilterSpec
    | WindowAggregateSpec
    | TemporalJoinSpec
    | IntervalJoinSpec
    | DeduplicateSpec
)


def _workflow_name(workflow: str | type[Any]) -> str:
    if isinstance(workflow, str):
        return workflow
    return getattr(workflow, "__highwater_workflow__", workflow.__name__)


def _value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return "none"
    return str(value)


def _process_node(
    definition: type[Any],
    *,
    input: str,
    process_id: str | None = None,
    options: ProcessOptions | None = None,
    direct_ingress: bool = False,
) -> ProcessSpec:
    """Lower a decorated Process into the same durable spec used by Client."""
    name = getattr(definition, "__highwater_process__", None)
    if name is None:
        raise TypeError(f"{definition.__name__} is missing @streaming.process")
    selected = options or ProcessOptions(
        key=getattr(definition, "__highwater_process_key__"),
        event_time_gate=getattr(definition, "__highwater_process_gate__"),
    )
    batch_size = selected.batch_size or getattr(
        definition, "__highwater_batch_max_size__"
    )
    batch_delay = (
        selected.batch_delay
        if selected.batch_delay is not None
        else getattr(definition, "__highwater_batch_max_delay__")
    )
    return ProcessSpec(
        process_id=process_id or name,
        input=input,
        workflow=definition,
        build_id=getattr(definition, "__highwater_build_id__"),
        state_version=getattr(definition, "__highwater_state_version__"),
        migrations_from=getattr(definition, "__highwater_migrations_from__"),
        versioned_streams=getattr(definition, "__highwater_versioned_streams__", ()),
        versioned_lookups=getattr(definition, "__highwater_versioned_lookups__", ()),
        key=selected.key,
        event_time=getattr(definition, "__highwater_process_event_time__"),
        event_time_gate=selected.event_time_gate,
        max_concurrency=selected.max_concurrency,
        capacity=selected.capacity,
        retry_concurrency=selected.retry_concurrency,
        max_attempts=selected.max_attempts,
        direct_ingress=direct_ingress,
        discard_input_on_success=selected.discard_input_on_success,
        batch_size=batch_size,
        batch_delay=batch_delay,
        task_queue=selected.task_queue,
    )


@dataclass(frozen=True)
class StreamDeclaration:
    name: str
    options: StreamOptions | None = None
    inferred: bool = False


@dataclass
class Dag:
    """A stable, inspectable view of Highwater deployment lowering."""

    name: str
    _streams: dict[str, StreamDeclaration] = field(default_factory=dict)
    _operators: dict[str, OperatorSpec] = field(default_factory=dict)
    _outputs: dict[str, str] = field(default_factory=dict)

    def stream(self, name: str, options: StreamOptions | None = None) -> "Dag":
        if not name.strip():
            raise ValueError("stream name must not be empty")
        current = self._streams.get(name)
        declaration = StreamDeclaration(name, options)
        if current is not None and current != declaration and not current.inferred:
            raise ValueError(f"stream {name!r} has conflicting declarations")
        self._streams[name] = declaration
        return self

    def _operator(self, spec: OperatorSpec, *, output: str | None = None) -> "Dag":
        operator_id = self._operator_id(spec)
        current = self._operators.get(operator_id)
        if current is not None and current != spec:
            raise ValueError(f"operator {operator_id!r} has conflicting specs")
        self._operators[operator_id] = spec
        for stream in self._input_streams(spec):
            self._streams.setdefault(stream, StreamDeclaration(stream, inferred=True))
        if output is not None:
            if not output.strip():
                raise ValueError("output stream must not be empty")
            self._outputs[operator_id] = output
            self._streams.setdefault(output, StreamDeclaration(output, inferred=True))
        return self

    def process(
        self,
        definition: type[Any],
        *,
        input: str,
        process_id: str | None = None,
        options: ProcessOptions | None = None,
        output: str | None = None,
    ) -> "Dag":
        return self._operator(
            _process_node(
                definition,
                input=input,
                process_id=process_id,
                options=options,
            ),
            output=output,
        )

    def filter(
        self,
        operator_id: str,
        *,
        input: str,
        workflow: str | type[Any],
        field: str,
        comparison: Comparison,
        operand: Any,
        task_queue: str = "default",
        output: str | None = None,
    ) -> "Dag":
        return self._operator(FilterSpec(
            operator_id=operator_id,
            stream=input,
            workflow=workflow,
            field=field,
            comparison=comparison,
            operand=operand,
            task_queue=task_queue,
        ), output=output)

    def window(
        self,
        operator_id: str,
        *,
        input: str,
        workflow: str | type[Any],
        size: float,
        start_at: float,
        aggregation: WindowAggregation = WindowAggregation.COUNT,
        slide: float | None = None,
        value: str | None = None,
        emit_empty: bool = False,
        task_queue: str = "default",
        output: str | None = None,
    ) -> "Dag":
        return self._operator(WindowAggregateSpec(
            operator_id=operator_id,
            stream=input,
            workflow=workflow,
            window_size=size,
            start_at=start_at,
            task_queue=task_queue,
            emit_empty_windows=emit_empty,
            aggregation=aggregation,
            slide=slide,
            value_field=value,
        ), output=output)

    def temporal_join(
        self,
        operator_id: str,
        *,
        probe: str,
        versions: str,
        workflow: str | type[Any],
        join_type: TemporalJoinType = TemporalJoinType.INNER,
        task_queue: str = "default",
        output: str | None = None,
    ) -> "Dag":
        return self._operator(TemporalJoinSpec(
            operator_id=operator_id,
            probe_stream=probe,
            version_stream=versions,
            workflow=workflow,
            task_queue=task_queue,
            join_type=join_type,
        ), output=output)

    def interval_join(
        self,
        operator_id: str,
        *,
        left: str,
        right: str,
        workflow: str | type[Any],
        lower: float,
        upper: float,
        join_type: IntervalJoinType = IntervalJoinType.INNER,
        task_queue: str = "default",
        output: str | None = None,
    ) -> "Dag":
        return self._operator(IntervalJoinSpec(
            operator_id=operator_id,
            left_stream=left,
            right_stream=right,
            workflow=workflow,
            lower_bound=lower,
            upper_bound=upper,
            task_queue=task_queue,
            join_type=join_type,
        ), output=output)

    def deduplicate(
        self,
        operator_id: str,
        *,
        input: str,
        workflow: str | type[Any],
        task_queue: str = "default",
        output: str | None = None,
    ) -> "Dag":
        return self._operator(DeduplicateSpec(
            operator_id=operator_id,
            stream=input,
            workflow=workflow,
            task_queue=task_queue,
        ), output=output)

    async def deploy(self, client: Any) -> None:
        """Create declared streams, operators, and changelog edges in dependency-safe order."""
        self.validate()
        for declaration in self.streams:
            if declaration.inferred:
                continue
            try:
                await client.stream_info(declaration.name)
            except RuntimeError as error:
                if "not found" not in str(error):
                    raise
                await client.create_stream(
                    declaration.name,
                    options=declaration.options,
                )
        for spec in self.operators:
            await client._deploy_operator(spec)
        for operator_id, output in sorted(self._outputs.items()):
            await client.connect_operator(operator_id, output)

    def snapshot(self) -> str:
        self.validate()
        lines = [f"dag {self.name}", "", "streams"]
        for name in sorted(self._streams):
            stream = self._streams[name]
            details = self._stream_details(stream)
            lines.append(f"  {name}{' ' + details if details else ''}")

        lines.extend(["", "operators"])
        for operator_id in sorted(self._operators):
            spec = self._operators[operator_id]
            lines.append(
                f"  {operator_id} [{self._operator_kind(spec)}] "
                f"{self._operator_details(spec)}"
            )

        lines.extend(["", "edges"])
        edges: list[tuple[str, str, str]] = []
        for operator_id, spec in self._operators.items():
            edges.extend(self._input_edges(operator_id, spec))
            output = self._outputs.get(operator_id)
            if output is not None:
                edges.append((operator_id, output, "changelog"))
        for source, target, details in sorted(edges):
            lines.append(f"  {source} -> {target} [{details}]")
        return "\n".join(lines) + "\n"

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("DAG name must not be empty")

        output_owners: dict[str, str] = {}
        for operator_id, output in self._outputs.items():
            previous = output_owners.get(output)
            if previous is not None and previous != operator_id:
                raise ValueError(
                    f"output stream {output!r} has multiple upstream operators"
                )
            output_owners[output] = operator_id
            declaration = self._streams[output]
            if declaration.options is None:
                raise ValueError(
                    f"output stream {output!r} requires an explicit declaration"
                )
            if declaration.options.watermark_mode.value != "source_managed":
                raise ValueError(
                    f"output stream {output!r} requires source-managed watermarks"
                )

        adjacency: dict[str, set[str]] = {}
        for operator_id, spec in self._operators.items():
            operator_node = f"operator:{operator_id}"
            for stream in self._input_streams(spec):
                adjacency.setdefault(f"stream:{stream}", set()).add(operator_node)
            output = self._outputs.get(operator_id)
            if output is not None:
                adjacency.setdefault(operator_node, set()).add(f"stream:{output}")

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visiting:
                raise ValueError("operator edges create a cycle")
            if node in visited:
                return
            visiting.add(node)
            for target in adjacency.get(node, ()):
                visit(target)
            visiting.remove(node)
            visited.add(node)

        for node in adjacency:
            visit(node)

    @property
    def streams(self) -> tuple[StreamDeclaration, ...]:
        return tuple(self._streams[name] for name in sorted(self._streams))

    @property
    def operators(self) -> tuple[OperatorSpec, ...]:
        return tuple(self._operators[name] for name in sorted(self._operators))

    @property
    def operator_kinds(self) -> tuple[str, ...]:
        return tuple(self._operator_kind(spec) for spec in self.operators)

    @property
    def edges(self) -> tuple[tuple[str, str, str], ...]:
        edges: list[tuple[str, str, str]] = []
        for operator_id, spec in self._operators.items():
            edges.extend(self._input_edges(operator_id, spec))
            output = self._outputs.get(operator_id)
            if output is not None:
                edges.append((operator_id, output, "changelog"))
        return tuple(sorted(edges))

    @staticmethod
    def _operator_id(spec: OperatorSpec) -> str:
        return spec.process_id if isinstance(spec, ProcessSpec) else spec.operator_id

    @staticmethod
    def _operator_kind(spec: OperatorSpec) -> str:
        names = {
            ProcessSpec: "process",
            FilterSpec: "filter",
            WindowAggregateSpec: "window aggregate",
            TemporalJoinSpec: "temporal join",
            IntervalJoinSpec: "interval join",
            DeduplicateSpec: "deduplicate",
        }
        return names[type(spec)]

    @staticmethod
    def _input_streams(spec: OperatorSpec) -> tuple[str, ...]:
        if isinstance(spec, ProcessSpec):
            return (spec.input, *spec.versioned_streams)
        if isinstance(spec, (FilterSpec, WindowAggregateSpec, DeduplicateSpec)):
            return (spec.stream,)
        if isinstance(spec, TemporalJoinSpec):
            return (spec.probe_stream, spec.version_stream)
        return (spec.left_stream, spec.right_stream)

    @staticmethod
    def _stream_details(stream: StreamDeclaration) -> str:
        if stream.options is None:
            return "[inferred]" if stream.inferred else ""
        defaults = StreamOptions()
        selected = []
        for item in fields(StreamOptions):
            value = getattr(stream.options, item.name)
            if value != getattr(defaults, item.name):
                selected.append(f"{item.name}={_value(value)}")
        return f"[{' '.join(selected)}]" if selected else "[defaults]"

    @staticmethod
    def _operator_details(spec: OperatorSpec) -> str:
        workflow = f"workflow={_workflow_name(spec.workflow)}"
        if isinstance(spec, ProcessSpec):
            details = [workflow, f"state=v{spec.state_version}"]
            if spec.key is not None:
                details.append(f"key={spec.key}")
            if spec.event_time is not None:
                details.append(f"event_time={spec.event_time}")
            details.append(f"gate={_value(spec.event_time_gate)}")
            details.append(f"batch={spec.batch_size}/{spec.batch_delay:g}s")
            if spec.migrations_from:
                details.append("migrates=" + ",".join(map(str, spec.migrations_from)))
            return " ".join(details)
        if isinstance(spec, FilterSpec):
            return (
                f"{workflow} predicate={spec.field} "
                f"{_value(spec.comparison)} {_value(spec.operand)}"
            )
        if isinstance(spec, WindowAggregateSpec):
            details = [
                workflow,
                f"aggregation={_value(spec.aggregation)}",
                f"size={spec.window_size:g}s",
                f"slide={(spec.slide or spec.window_size):g}s",
                f"start={spec.start_at:g}",
            ]
            if spec.value_field is not None:
                details.append(f"value={spec.value_field}")
            return " ".join(details)
        if isinstance(spec, TemporalJoinSpec):
            return f"{workflow} type={_value(spec.join_type)} as_of=probe.event_time"
        if isinstance(spec, IntervalJoinSpec):
            return (
                f"{workflow} type={_value(spec.join_type)} "
                f"bounds=[{spec.lower_bound:g}s,{spec.upper_bound:g}s]"
            )
        return f"{workflow} keep=first_by_event_time"

    @staticmethod
    def _input_edges(
        operator_id: str, spec: OperatorSpec
    ) -> list[tuple[str, str, str]]:
        if isinstance(spec, ProcessSpec):
            event_details = ["events"]
            if spec.key is not None:
                event_details.append(f"key={spec.key}")
            if spec.event_time is not None:
                event_details.append(f"event_time={spec.event_time}")
            edges = [(spec.input, operator_id, " ".join(event_details))]
            lookup_keys = dict(spec.versioned_lookups)
            for stream in spec.versioned_streams:
                details = "versioned lookup"
                if stream in lookup_keys:
                    details += f" key={lookup_keys[stream]}"
                details += " as_of=event_time completeness=required"
                edges.append((stream, operator_id, details))
            return edges
        if isinstance(spec, FilterSpec):
            return [(spec.stream, operator_id, "records")]
        if isinstance(spec, WindowAggregateSpec):
            return [(spec.stream, operator_id, "keyed event-time records")]
        if isinstance(spec, TemporalJoinSpec):
            return [
                (spec.probe_stream, operator_id, "probe"),
                (spec.version_stream, operator_id, "versions key=record.key"),
            ]
        if isinstance(spec, IntervalJoinSpec):
            return [
                (spec.left_stream, operator_id, "left key=record.key"),
                (spec.right_stream, operator_id, "right key=record.key"),
            ]
        return [(spec.stream, operator_id, "keyed event-time records")]
