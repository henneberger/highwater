from __future__ import annotations

import unittest

from benchmarks.netherite_partition_throughput import assigned_partitions
from highwater.registry import Registry
from highwater.rust_worker import RustWorker


class PartitionExecutionTest(unittest.TestCase):
    def test_assigns_every_partition_to_one_execution_instance(self) -> None:
        assignments = assigned_partitions(10, 3)

        self.assertEqual(
            assignments,
            [[1, 4, 7, 10], [2, 5, 8], [3, 6, 9]],
        )
        self.assertEqual(
            sorted(partition for assignment in assignments for partition in assignment),
            list(range(1, 11)),
        )

    def test_does_not_create_empty_execution_instances(self) -> None:
        self.assertEqual(assigned_partitions(2, 5), [[1], [2]])

    def test_rejects_duplicate_or_control_partitions(self) -> None:
        with self.assertRaisesRegex(ValueError, "unique positive integers"):
            RustWorker(Registry(), process_partitions=(1, 1))
        with self.assertRaisesRegex(ValueError, "unique positive integers"):
            RustWorker(Registry(), process_partitions=(0,))


if __name__ == "__main__":
    unittest.main()
