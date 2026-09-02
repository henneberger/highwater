from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass, field

from highwater import (
    AutoscalingPolicy,
    StreamRecord,
    WorkloadSample,
    assign_partitions,
    compare_process_builds,
    recommend_replicas,
    streaming,
)


catalog = streaming.versioned("replay-catalog", key="product_id")


@streaming.process(key="user_id", build_id="replay-v1")
@dataclass
class ReplayV1:
    recent: list[str] = field(default_factory=list)

    @streaming.event
    async def apply(self, event, context):
        product = await catalog.get(event.product_id, as_of=context.event_time)
        self.recent.append(product.category)
        return {"recent": self.recent}


@streaming.process(key="user_id", build_id="replay-v2")
@dataclass
class ReplayV2:
    recent: list[str] = field(default_factory=list)

    @streaming.event
    async def apply(self, event, context):
        product = await catalog.get(event.product_id, as_of=context.event_time)
        self.recent.append(product.category)
        self.recent = self.recent[-1:]
        return {"recent": self.recent}


def record(
    sequence: int,
    *,
    stream: str,
    key: str,
    value: dict,
    event_time: float,
) -> StreamRecord:
    return StreamRecord(
        stream=stream,
        partition=0,
        offset=sequence - 1,
        sequence=sequence,
        event_time=event_time,
        ingestion_time=event_time,
        key=key,
        value=value,
        kind="upsert",
        event_id=f"event-{sequence}",
        key_group=0,
        owner_epoch=1,
        source_id=None,
        source_partition=None,
        source_offset=None,
        late=False,
        too_late=False,
    )


class ScalingAndReplayTest(unittest.TestCase):
    def test_autoscaler_adds_capacity_and_balances_partitions(self) -> None:
        decision = recommend_replicas(
            WorkloadSample(0, pending=0, running=0, completed=100),
            WorkloadSample(1, pending=20_000, running=100, completed=5_100),
            current_replicas=2,
            partitions=10,
            policy=AutoscalingPolicy(
                max_replicas=10,
                target_events_per_second_per_replica=5_000,
                target_backlog_per_replica=5_000,
            ),
        )
        self.assertEqual(decision.desired_replicas, 7)
        self.assertEqual(decision.reason, "traffic")
        self.assertEqual(
            sorted(partition for group in decision.partition_assignments for partition in group),
            list(range(1, 11)),
        )
        self.assertLessEqual(
            max(map(len, decision.partition_assignments))
            - min(map(len, decision.partition_assignments)),
            1,
        )

    def test_autoscaler_scales_down_one_replica_after_stabilization(self) -> None:
        policy = AutoscalingPolicy(min_replicas=1, max_replicas=8, scale_down_after=60)
        decision = recommend_replicas(
            WorkloadSample(0, pending=0, running=0, completed=100),
            WorkloadSample(60, pending=0, running=0, completed=100),
            current_replicas=4,
            partitions=8,
            policy=policy,
            seconds_below_target=60,
        )
        self.assertEqual(decision.desired_replicas, 3)
        self.assertEqual(decision.reason, "sustained spare capacity")
        self.assertEqual(assign_partitions(8, 3), decision.partition_assignments)

    def test_autoscaler_removes_replicas_that_cannot_own_a_partition(self) -> None:
        decision = recommend_replicas(
            WorkloadSample(0, pending=0, running=0, completed=100),
            WorkloadSample(10, pending=0, running=0, completed=100),
            current_replicas=8,
            partitions=4,
        )
        self.assertEqual(decision.desired_replicas, 4)
        self.assertEqual(decision.reason, "partition ceiling")
        self.assertEqual(decision.partition_assignments, ((1,), (2,), (3,), (4,)))

    def test_replay_comparison_uses_versioned_state_and_reports_exact_diff(self) -> None:
        source = [
            record(1, stream="views", key="user-1", value={"user_id": "user-1", "product_id": "p1"}, event_time=10),
            record(2, stream="views", key="user-1", value={"user_id": "user-1", "product_id": "p2"}, event_time=20),
        ]
        versions = [
            record(1, stream="replay-catalog", key="p1", value={"category": "books"}, event_time=5),
            record(2, stream="replay-catalog", key="p2", value={"category": "games"}, event_time=15),
        ]

        matching = asyncio.run(compare_process_builds(
            ReplayV1,
            ReplayV1,
            source,
            versioned_histories={"replay-catalog": versions},
        ))
        self.assertTrue(matching.matches)
        self.assertEqual(matching.matching_events, 2)

        changed = asyncio.run(compare_process_builds(
            ReplayV1,
            ReplayV2,
            source,
            versioned_histories={"replay-catalog": versions},
        ))
        self.assertFalse(changed.matches)
        self.assertEqual(changed.matching_events, 1)
        self.assertEqual(len(changed.differences), 1)
        difference = changed.differences[0]
        self.assertEqual(difference.event_id, "event-2")
        self.assertEqual(difference.baseline_state, {"recent": ["books", "games"]})
        self.assertEqual(difference.candidate_state, {"recent": ["games"]})


if __name__ == "__main__":
    unittest.main()
