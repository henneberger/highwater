from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from math import isfinite
from typing import Any


class WorkflowStatus(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    TERMINATED = "TERMINATED"


class LatePolicy(StrEnum):
    DROP = "drop"
    SIDE_OUTPUT = "side_output"
    ACCEPT = "accept"


class WatermarkMode(StrEnum):
    BOUNDED = "bounded"
    MONOTONIC = "monotonic"
    SOURCE_MANAGED = "source_managed"


class ChangeKind(StrEnum):
    INSERT = "insert"
    UPSERT = "upsert"
    UPDATE_BEFORE = "update_before"
    UPDATE_AFTER = "update_after"
    DELETE = "delete"


class EventTimeGate(StrEnum):
    IMMEDIATE = "immediate"
    COMPLETE = "complete"


class TemporalJoinType(StrEnum):
    INNER = "inner"
    LEFT = "left"


class IntervalJoinType(StrEnum):
    INNER = "inner"


class WindowAggregation(StrEnum):
    COUNT = "count"
    SUM = "sum"
    MAX = "max"


class Comparison(StrEnum):
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"


@dataclass(frozen=True)
class Event:
    id: int
    workflow_id: str
    type: str
    data: dict[str, Any]
    created_at: float


@dataclass(frozen=True)
class RetryPolicy:
    maximum_attempts: int = 3
    initial_interval: float = 0.1
    backoff_coefficient: float = 2.0
    maximum_interval: float = 30.0

    def delay(self, attempt: int) -> float:
        return min(self.initial_interval * self.backoff_coefficient ** (attempt - 1), self.maximum_interval)


@dataclass(frozen=True)
class ActivityOptions:
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    task_queue: str | None = None
    schedule_to_close_timeout: float | None = None
    start_to_close_timeout: float | None = None
    heartbeat_timeout: float | None = None


@dataclass(frozen=True)
class ChildWorkflowOptions:
    workflow_id: str | None = None
    parent_close_policy: str = "TERMINATE"


@dataclass(frozen=True)
class WorkflowOptions:
    task_queue: str = "default"
    execution_timeout: float | None = None


@dataclass(frozen=True)
class StreamOptions:
    partitions: int = 1
    watermark_mode: WatermarkMode = WatermarkMode.BOUNDED
    max_out_of_orderness: float | timedelta = 5.0
    idle_timeout: float | timedelta | None = 60.0
    allowed_lateness: float | timedelta = 0.0
    alignment_max_drift: float | timedelta | None = None
    late_policy: LatePolicy = LatePolicy.SIDE_OUTPUT


@dataclass(frozen=True)
class StreamRecord:
    stream: str
    partition: int
    offset: int
    sequence: int
    event_time: float
    ingestion_time: float
    key: str | None
    value: Any
    kind: str
    event_id: str | None
    key_group: int
    owner_epoch: int
    source_id: str | None
    source_partition: int | None
    source_offset: int | None
    late: bool
    too_late: bool


@dataclass(frozen=True)
class StreamInfo:
    config: dict[str, Any]
    watermark: float | None
    finalized: bool
    max_event_time: float | None
    partitions: list[dict[str, Any]]
    watermark_diagnostics: dict[str, Any]


@dataclass(frozen=True)
class ProcessSpec:
    process_id: str
    input: str
    workflow: str | type[Any]
    build_id: str
    state_version: int = 1
    migrations_from: tuple[int, ...] = ()
    versioned_streams: tuple[str, ...] = ()
    versioned_lookups: tuple[tuple[str, str], ...] = ()
    key: str | None = None
    event_time: str | None = None
    event_time_gate: EventTimeGate = EventTimeGate.IMMEDIATE
    max_concurrency: int = 64
    capacity: int = 10_000
    retry_concurrency: int = 8
    max_attempts: int = 5
    direct_ingress: bool = False
    discard_input_on_success: bool = False
    batch_size: int = 64
    batch_delay: float = 0.005
    task_queue: str = "default"

    def __post_init__(self) -> None:
        if not self.process_id.strip() or not self.input.strip() or not self.build_id.strip():
            raise ValueError("process_id, input, and build_id must not be empty")
        if self.state_version <= 0:
            raise ValueError("state_version must be positive")
        if (
            self.max_concurrency <= 0
            or self.capacity <= 0
            or self.retry_concurrency <= 0
            or self.max_attempts <= 0
        ):
            raise ValueError(
                "max_concurrency, capacity, retry_concurrency, and max_attempts must be positive"
            )
        if (
            not 1 <= self.batch_size <= 16_384
            or not isfinite(self.batch_delay)
            or self.batch_delay < 0
        ):
            raise ValueError("batch_size must be 1..16384 and batch_delay finite and non-negative")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")
        if self.key is not None and not self.key.strip():
            raise ValueError("key must not be empty")
        if self.event_time is not None and not self.event_time.strip():
            raise ValueError("event_time must not be empty")
        if any(not stream.strip() for stream in self.versioned_streams):
            raise ValueError("versioned streams must not be empty")
        if len(set(self.versioned_streams)) != len(self.versioned_streams):
            raise ValueError("versioned streams must be unique")
        if any(not stream.strip() or not key.strip() for stream, key in self.versioned_lookups):
            raise ValueError("versioned lookup streams and keys must not be empty")
        if len(set(self.versioned_lookups)) != len(self.versioned_lookups):
            raise ValueError("versioned lookups must be unique")
        if any(stream not in self.versioned_streams for stream, _ in self.versioned_lookups):
            raise ValueError("versioned lookup streams must be declared dependencies")


@dataclass(frozen=True)
class ProcessOptions:
    key: str | None = None
    event_time_gate: EventTimeGate = EventTimeGate.IMMEDIATE
    max_concurrency: int = 64
    capacity: int = 10_000
    retry_concurrency: int = 8
    max_attempts: int = 5
    discard_input_on_success: bool = False
    batch_size: int | None = None
    batch_delay: float | None = None
    task_queue: str = "default"

    def __post_init__(self) -> None:
        if self.key is not None and not self.key.strip():
            raise ValueError("key must not be empty")
        if (
            self.max_concurrency <= 0
            or self.capacity <= 0
            or self.retry_concurrency <= 0
            or self.max_attempts <= 0
        ):
            raise ValueError(
                "max_concurrency, capacity, retry_concurrency, and max_attempts must be positive"
            )
        if self.batch_size is not None and not 1 <= self.batch_size <= 16_384:
            raise ValueError("batch_size must be between 1 and 16384")
        if self.batch_delay is not None and (
            not isfinite(self.batch_delay) or self.batch_delay < 0
        ):
            raise ValueError("batch_delay must be finite and non-negative")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")


@dataclass(frozen=True)
class FilterSpec:
    operator_id: str
    stream: str
    workflow: str | type[Any]
    field: str
    comparison: Comparison
    operand: Any
    task_queue: str = "default"

    def __post_init__(self) -> None:
        if not self.operator_id.strip() or not self.stream.strip() or not self.field.strip():
            raise ValueError("operator_id, stream, and field must not be empty")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")


@dataclass(frozen=True)
class WindowAggregateSpec:
    operator_id: str
    stream: str
    workflow: str | type[Any]
    window_size: float
    start_at: float
    task_queue: str = "default"
    emit_empty_windows: bool = False
    aggregation: WindowAggregation = WindowAggregation.COUNT
    slide: float | None = None
    value_field: str | None = None

    def __post_init__(self) -> None:
        if not self.operator_id.strip() or not self.stream.strip():
            raise ValueError("operator_id and stream must not be empty")
        if self.window_size <= 0 or not isfinite(self.window_size):
            raise ValueError("window_size must be finite and positive")
        if not isfinite(self.start_at):
            raise ValueError("start_at must be finite")
        if self.slide is not None and (
            self.slide <= 0
            or not isfinite(self.slide)
            or self.slide > self.window_size
        ):
            raise ValueError("slide must be finite, positive, and no larger than window_size")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")
        if self.value_field is not None and not self.value_field.strip():
            raise ValueError("value_field must not be empty")


@dataclass(frozen=True)
class TemporalJoinSpec:
    operator_id: str
    probe_stream: str
    version_stream: str
    workflow: str | type[Any]
    task_queue: str = "default"
    join_type: TemporalJoinType = TemporalJoinType.INNER

    def __post_init__(self) -> None:
        if not self.operator_id.strip():
            raise ValueError("operator_id must not be empty")
        if not self.probe_stream.strip() or not self.version_stream.strip():
            raise ValueError("temporal join streams must not be empty")
        if self.probe_stream == self.version_stream:
            raise ValueError("probe_stream and version_stream must be different")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")


@dataclass(frozen=True)
class IntervalJoinSpec:
    operator_id: str
    left_stream: str
    right_stream: str
    workflow: str | type[Any]
    lower_bound: float
    upper_bound: float
    task_queue: str = "default"
    join_type: IntervalJoinType = IntervalJoinType.INNER

    def __post_init__(self) -> None:
        if not self.operator_id.strip():
            raise ValueError("operator_id must not be empty")
        if not self.left_stream.strip() or not self.right_stream.strip():
            raise ValueError("interval join streams must not be empty")
        if (
            not isfinite(self.lower_bound)
            or not isfinite(self.upper_bound)
            or self.lower_bound > self.upper_bound
        ):
            raise ValueError("bounds must be finite and lower_bound <= upper_bound")
        if self.left_stream == self.right_stream and self.lower_bound < 0:
            raise ValueError("ordered self interval joins require a non-negative lower_bound")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")


@dataclass(frozen=True)
class DeduplicateSpec:
    operator_id: str
    stream: str
    workflow: str | type[Any]
    task_queue: str = "default"

    def __post_init__(self) -> None:
        if not self.operator_id.strip() or not self.stream.strip():
            raise ValueError("operator_id and stream must not be empty")
        if not self.task_queue.strip():
            raise ValueError("task_queue must not be empty")


@dataclass(frozen=True)
class TemporalJoinOutput:
    join_id: str
    probe: StreamRecord
    version: StreamRecord | None
    as_of: float
    watermark: float | None
    workflow_id: str | None


@dataclass(frozen=True)
class IntervalJoinOutput:
    join_id: str
    left: StreamRecord
    right: StreamRecord
    workflow_id: str


@dataclass(frozen=True)
class DeduplicateOutput:
    operator_id: str
    record: StreamRecord
    canonical: bool
    canonical_record: StreamRecord
    workflow_id: str | None


@dataclass(frozen=True)
class Command:
    type: str
    attributes: dict[str, Any]


@dataclass(frozen=True)
class ActivationResult:
    commands: list[Command]
    blocked: bool
    instance: Any
    history_event_id: int
