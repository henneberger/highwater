from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkloadSample:
    observed_at: float
    pending: int
    running: int
    completed: int
    oldest_unfinished_age_seconds: float | None = None
    retrying: int = 0

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at):
            raise ValueError("sample time must be finite")
        if min(self.pending, self.running, self.completed, self.retrying) < 0:
            raise ValueError("workload counters must be non-negative")
        if self.oldest_unfinished_age_seconds is not None and (
            not math.isfinite(self.oldest_unfinished_age_seconds)
            or self.oldest_unfinished_age_seconds < 0
        ):
            raise ValueError("oldest unfinished age must be finite and non-negative")


@dataclass(frozen=True)
class AutoscalingPolicy:
    min_replicas: int = 0
    max_replicas: int = 64
    target_events_per_second_per_replica: float = 5_000
    target_backlog_per_replica: int = 5_000
    headroom: float = 1.25
    scale_down_after: float = 300
    latency_target_seconds: float | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.min_replicas <= self.max_replicas:
            raise ValueError("replica bounds are invalid")
        if self.target_events_per_second_per_replica <= 0:
            raise ValueError("target throughput must be positive")
        if self.target_backlog_per_replica <= 0:
            raise ValueError("target backlog must be positive")
        if self.headroom < 1:
            raise ValueError("autoscaling headroom must be at least one")
        if self.latency_target_seconds is not None and (
            not math.isfinite(self.latency_target_seconds) or self.latency_target_seconds <= 0
        ):
            raise ValueError("latency target must be finite and positive")
        if self.scale_down_after < 0:
            raise ValueError("scale-down delay must be non-negative")


@dataclass(frozen=True)
class ScalingDecision:
    current_replicas: int
    desired_replicas: int
    incoming_events_per_second: float
    completed_events_per_second: float
    reason: str
    partition_assignments: tuple[tuple[int, ...], ...]
    oldest_unfinished_age_seconds: float | None = None
    latency_target_seconds: float | None = None
    latency_target_exceeded: bool | None = None


def assign_partitions(partitions: int, replicas: int) -> tuple[tuple[int, ...], ...]:
    if partitions <= 0 or replicas < 0:
        raise ValueError("partitions must be positive and replicas must be non-negative")
    if replicas == 0:
        return ()
    active = min(partitions, replicas)
    assignments = [[] for _ in range(active)]
    for index, partition in enumerate(range(1, partitions + 1)):
        assignments[index % active].append(partition)
    return tuple(tuple(values) for values in assignments)


def recommend_replicas(
    previous: WorkloadSample,
    current: WorkloadSample,
    *,
    current_replicas: int,
    partitions: int,
    policy: AutoscalingPolicy | None = None,
    seconds_below_target: float = 0,
) -> ScalingDecision:
    selected = policy or AutoscalingPolicy()
    if current_replicas < 0:
        raise ValueError("current replicas must be non-negative")
    elapsed = current.observed_at - previous.observed_at
    if elapsed <= 0:
        raise ValueError("samples must advance in time")
    if current.completed < previous.completed:
        raise ValueError("completed work cannot regress")

    completed_rate = (current.completed - previous.completed) / elapsed
    admitted_delta = (
        current.pending
        + current.running
        + current.retrying
        + current.completed
        - previous.pending
        - previous.running
        - previous.retrying
        - previous.completed
    )
    incoming_rate = max(0.0, admitted_delta / elapsed)
    traffic_replicas = math.ceil(
        incoming_rate
        * selected.headroom
        / selected.target_events_per_second_per_replica
    )
    backlog_replicas = math.ceil(
        current.pending * selected.headroom / selected.target_backlog_per_replica
    )
    latency_replicas = 0
    exceeded = None
    if selected.latency_target_seconds is not None:
        age = current.oldest_unfinished_age_seconds
        if age is None:
            raise ValueError("latency-aware scaling requires oldest unfinished age telemetry")
        exceeded = age > selected.latency_target_seconds
        unfinished = current.pending + current.running + current.retrying
        if unfinished:
            remaining = selected.latency_target_seconds - age
            if remaining <= 0:
                latency_replicas = min(selected.max_replicas, partitions)
            else:
                latency_replicas = math.ceil(
                    unfinished * selected.headroom
                    / (remaining * selected.target_events_per_second_per_replica)
                )
    calculated = max(
        selected.min_replicas, traffic_replicas, backlog_replicas, latency_replicas
    )
    calculated = min(selected.max_replicas, partitions, calculated)

    if current_replicas > partitions:
        desired = partitions
        reason = "partition ceiling"
    elif calculated > current_replicas:
        desired = calculated
        reason = (
            "latency" if latency_replicas > max(traffic_replicas, backlog_replicas)
            else "traffic" if traffic_replicas >= backlog_replicas else "backlog"
        )
    elif calculated < current_replicas:
        if (current.pending + current.running + current.retrying > 0
            or seconds_below_target < selected.scale_down_after):
            desired = current_replicas
            reason = "scale-down stabilization"
        else:
            desired = max(calculated, current_replicas - 1)
            reason = "sustained spare capacity"
    else:
        desired = current_replicas
        reason = "within target"

    return ScalingDecision(
        current_replicas=current_replicas,
        desired_replicas=desired,
        incoming_events_per_second=incoming_rate,
        completed_events_per_second=completed_rate,
        reason=reason,
        partition_assignments=assign_partitions(partitions, desired),
        oldest_unfinished_age_seconds=current.oldest_unfinished_age_seconds,
        latency_target_seconds=selected.latency_target_seconds,
        latency_target_exceeded=exceeded,
    )
