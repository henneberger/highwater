from __future__ import annotations

import difflib
import unittest
from pathlib import Path

from examples.dags import ALL
from examples.ai_support_operations import topology as ai_support_topology
from highwater import StreamOptions, WatermarkMode, streaming
from highwater.dag import Dag


SNAPSHOT_DIRECTORY = Path(__file__).parent / "snapshots" / "dags"


class DagSnapshotTests(unittest.TestCase):
    def test_example_dags(self) -> None:
        for name, build in ALL.items():
            with self.subTest(name=name):
                actual = build().snapshot()
                path = SNAPSHOT_DIRECTORY / f"{name}.txt"
                expected = path.read_text()
                self.assertEqual(
                    actual,
                    expected,
                    "DAG lowering changed:\n" + "".join(difflib.unified_diff(
                        expected.splitlines(keepends=True),
                        actual.splitlines(keepends=True),
                        fromfile=str(path),
                        tofile=f"generated:{name}",
                    )),
                )

    def test_complex_ai_topology_has_expected_structure(self) -> None:
        dag = ai_support_topology()
        dag.validate()

        self.assertEqual(len(dag.streams), 14)
        self.assertEqual(len(dag.operators), 9)
        self.assertEqual(len(dag.edges), 22)
        self.assertEqual(
            set(dag.operator_kinds),
            {
                "deduplicate",
                "filter",
                "interval join",
                "process",
                "temporal join",
                "window aggregate",
            },
        )

        edges = set(dag.edges)
        self.assertIn(
            (
                "support-knowledge-versions",
                "support-triage",
                "versioned lookup key=topic as_of=event_time completeness=required",
            ),
            edges,
        )
        self.assertIn(
            ("support-triage", "agent-decisions", "changelog"),
            edges,
        )
        self.assertIn(
            ("agent-decisions", "match-decisions-to-feedback", "left key=record.key"),
            edges,
        )
        self.assertIn(
            ("routed-escalations", "coordinate-handoffs", "events key=customer_id"),
            edges,
        )
        self.assertIn(
            ("escalations", "route-escalations", "probe"),
            edges,
        )
        self.assertIn(
            (
                "service-plan-versions",
                "route-escalations",
                "versions key=record.key",
            ),
            edges,
        )

    def test_ahead_of_time_cycles_include_both_temporal_forms(self) -> None:
        options = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED)
        reference = streaming.versioned("reference", key="id")

        @streaming.process(key="id")
        class Process:
            @streaming.event
            async def apply(self, event, context):
                return await reference.get(event.id, as_of=context.event_time)

        process_dag = (
            Dag("process-reference-cycle")
            .stream("events", options)
            .stream("reference", options)
            .process(
                Process,
                input="events",
                process_id="process",
                output="reference",
            )
        )
        with self.assertRaisesRegex(ValueError, "create a cycle"):
            process_dag.validate()

        temporal_dag = (
            Dag("temporal-join-cycle")
            .stream("probes", options)
            .stream("versions", options)
            .temporal_join(
                "join",
                probe="probes",
                versions="versions",
                workflow="Join",
                output="probes",
            )
        )
        with self.assertRaisesRegex(ValueError, "create a cycle"):
            temporal_dag.validate()


if __name__ == "__main__":
    unittest.main()
