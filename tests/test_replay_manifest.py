from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import unittest

from highwater import streaming
from highwater.client import Client
from highwater.replay import ReplayManifest, compare_process_builds
from test_scaling_and_replay import ReplayV1, record


@streaming.process(key="id", build_id="manifest-v1")
@dataclass
class Append:
    values: list[int] = field(default_factory=list)

    @streaming.event
    async def apply(self, event, context):
        self.values.append(event.value)
        return {"values": self.values}


@streaming.process(key="id", build_id="manifest-v2", state_version=2)
@dataclass
class Migrated:
    values: list[int] = field(default_factory=list)

    @streaming.migrate(from_version=1)
    def migrate(self, state):
        state["values"].append(99)
        return state

    @streaming.event
    async def apply(self, event, context):
        self.values.append(event.value)
        return {"values": self.values}


class ReplayManifestTest(unittest.TestCase):
    def source(self):
        return [record(i, stream="source", key="a", value={"value": i}, event_time=i)
                for i in (1, 2)]

    def test_round_trip_reuses_pinned_inputs_and_checks_checksum(self):
        source = self.source()
        comparison = asyncio.run(compare_process_builds(Append, Append, source))
        manifest = ReplayManifest.from_json(comparison.manifest.to_json())
        source[0].value["value"] = 999
        self.assertEqual(manifest.inputs()["records"][0]["value"], {"value": 1})
        replayed = asyncio.run(compare_process_builds(Append, Append, manifest=manifest))
        self.assertTrue(replayed.matches)
        self.assertEqual(replayed.manifest.sha256, comparison.manifest.sha256)
        changed = json.loads(manifest.to_json())
        changed["payload"]["records"][0]["value"] = {"value": 999}
        with self.assertRaisesRegex(ValueError, "checksum"):
            ReplayManifest.from_json(json.dumps(changed))

    def test_initial_state_migrations_and_event_results_are_isolated(self):
        states = {"a": {"values": [0]}}
        comparison = asyncio.run(compare_process_builds(
            Append, Migrated, self.source(), initial_states=states, initial_state_version=1,
        ))
        self.assertEqual(states, {"a": {"values": [0]}})
        first, second = comparison.differences
        self.assertEqual(first.baseline_state, {"values": [0, 1]})
        self.assertEqual(first.candidate_state, {"values": [0, 99, 1]})
        self.assertEqual(second.candidate_state, {"values": [0, 99, 1, 2]})
        self.assertEqual(first.baseline_output, {"values": [0, 1]})

    def test_rejects_new_inputs_or_wrong_build_for_saved_manifest(self):
        manifest = ReplayManifest.capture(Append, Append, self.source())
        with self.assertRaisesRegex(ValueError, "build ID"):
            asyncio.run(compare_process_builds(Append, Migrated, manifest=manifest))
        with self.assertRaisesRegex(ValueError, "new replay inputs"):
            asyncio.run(compare_process_builds(Append, Append, [], manifest=manifest))

    def test_missing_history_and_unversioned_initial_state_fail_before_execution(self):
        with self.assertRaisesRegex(ValueError, "captured histories"):
            ReplayManifest.capture(ReplayV1, ReplayV1, [])
        with self.assertRaisesRegex(ValueError, "initial_state_version"):
            ReplayManifest.capture(Append, Append, [], initial_states={"a": {"values": []}})

    def test_client_reuses_manifest_without_server_reads(self):
        manifest = ReplayManifest.capture(Append, Append, self.source())
        client = Client("http://unreachable.invalid")
        comparison = asyncio.run(client.compare_builds(
            "unused", baseline=Append, candidate=Append, manifest=manifest,
        ))
        self.assertTrue(comparison.matches)


if __name__ == "__main__":
    unittest.main()
