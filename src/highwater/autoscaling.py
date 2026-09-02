from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkloadSample:
    observed_at: float
    pending: int
    running: int
    completed: int

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at):
            raise ValueError("sample time must be finite")
        if min(self.pending, self.running, self.completed) < 0:
            raise ValueError("workload counters must be non-negative")


@dataclass(frozen=True)
class AutoscalingPolicy:
    min_replicas: int = 1
    max_replicas: int = 64
    target_events_per_second_per_replica: float = 5_000
    target_backlog_per_replica: int = 5_000
    headroom: float = 1.25
    scale_down_after: float = 300

    def __post_init__(self) -> None:
        if not 1 <= self.min_replicas <= self.max_replicas:
            raise ValueError("replica bounds are invalid")
        if self.target_events_per_second_per_replica <= 0:
            raise ValueError("target throughput must be positive")
        if self.target_backlog_per_replica <= 0:
            raise ValueError("target backlog must be positive")
        if self.headroom < 1:
            raise ValueError("autoscaling headroom must be at least one")
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


def assign_partitions(partitions: int, replicas: int) -> tuple[tuple[int, ...], ...]:
    if partitions <= 0 or replicas <= 0:
        raise ValueError("partitions and replicas must be positive")
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
    if current_replicas <= 0:
        raise ValueError("current replicas must be positive")
    elapsed = current.observed_at - previous.observed_at
    if elapsed <= 0:
        raise ValueError("samples must advance in time")
    if current.completed < previous.completed:
        raise ValueError("completed work cannot regress")

    completed_rate = (current.completed - previous.completed) / elapsed
    admitted_delta = (
        current.pending
        + current.running
        + current.completed
        - previous.pending
        - previous.running
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
    calculated = max(selected.min_replicas, traffic_replicas, backlog_replicas)
    calculated = min(selected.max_replicas, partitions, calculated)

    if current_replicas > partitions:
        desired = partitions
        reason = "partition ceiling"
    elif calculated > current_replicas:
        desired = calculated
        reason = "traffic" if traffic_replicas >= backlog_replicas else "backlog"
    elif calculated < current_replicas:
        if current.pending > 0 or seconds_below_target < selected.scale_down_after:
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
    )
