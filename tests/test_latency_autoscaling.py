from __future__ import annotations

import unittest

from highwater.autoscaler_cli import _sample
from highwater.autoscaling import AutoscalingPolicy, WorkloadSample, recommend_replicas


class LatencyAutoscalingTest(unittest.TestCase):
    def decision(self, age, *, running=0, pending=100, retrying=0):
        return recommend_replicas(
            WorkloadSample(0, pending, running, 0, age, retrying),
            WorkloadSample(1, pending, running, 0, age, retrying),
            current_replicas=1,
            partitions=8,
            policy=AutoscalingPolicy(
                latency_target_seconds=1,
                target_events_per_second_per_replica=1000,
                headroom=1,
            ),
        )

    def test_remaining_budget_scales_before_deadline(self):
        self.assertEqual(self.decision(0.5).desired_replicas, 1)
        near = self.decision(0.96)
        self.assertEqual(near.desired_replicas, 3)
        self.assertEqual(near.reason, "latency")
        self.assertFalse(near.latency_target_exceeded)

    def test_exhausted_budget_scales_to_partition_ceiling(self):
        decision = self.decision(1.1)
        self.assertEqual(decision.desired_replicas, 8)
        self.assertTrue(decision.latency_target_exceeded)

    def test_running_and_retrying_work_retain_age_pressure(self):
        self.assertEqual(self.decision(2, running=1, pending=0).desired_replicas, 8)
        self.assertEqual(self.decision(2, retrying=1, pending=0).desired_replicas, 8)

    def test_missing_telemetry_is_not_treated_as_fresh_work(self):
        with self.assertRaisesRegex(ValueError, "telemetry"):
            self.decision(None)

    def test_idle_target_does_not_create_work(self):
        decision = self.decision(0, pending=0)
        self.assertFalse(decision.latency_target_exceeded)
        self.assertEqual(decision.desired_replicas, 1)  # stabilization

    def test_validates_nonfinite_targets_and_ages(self):
        for value in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                AutoscalingPolicy(latency_target_seconds=value)
        for value in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                WorkloadSample(0, 0, 0, 0, value)

    def test_cli_extracts_server_age_and_retry_count(self):
        sample = _sample({"pending": 0, "running": 1, "completed": 2,
                          "retrying": 3, "latency": {"oldest_unfinished_age_seconds": 4}}, 10)
        self.assertEqual(sample.oldest_unfinished_age_seconds, 4)
        self.assertEqual(sample.retrying, 3)


if __name__ == "__main__":
    unittest.main()
