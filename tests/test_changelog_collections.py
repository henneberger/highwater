from __future__ import annotations

import asyncio
import socket
import subprocess
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

from highwater import Client, CollectionMode, Registry, StreamOptions, WatermarkMode, streaming
from highwater.dag import Dag
from highwater.model import TopNSpec
from highwater.rust_worker import RustWorker

SERVER = Path(__file__).resolve().parents[1] / "target/debug/highwater-server"
MANAGED = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED, idle_timeout=None)


@streaming.process(key="id", build_id="collection-score-v1")
@dataclass
class CurrentScore:
    score: int = 0

    @streaming.event
    async def apply(self, event):
        self.score = event.score
        return {"id": event.id, "score": self.score}


class CollectionSpecTests(unittest.TestCase):
    def test_validation_precedes_deployment_and_plan_explains_strategy(self):
        for changes in ({"n": True}, {"n": 0}, {"order_by": []},
                        {"order_by": [("score", "sideways")]},
                        {"input_mode": CollectionMode.UPSERT}, {"partition_by": "league"}):
            args = dict(operator_id="n", stream="s", order_by=[("score", "desc")], n=2)
            args.update(changes)
            with self.assertRaises(ValueError):
                TopNSpec(**args)
        dag = Dag("leaders").stream("raw", MANAGED).stream("changes", MANAGED)
        dag.top_n("top", input="raw", order_by=[("score", "desc")], n=2, output="changes")
        self.assertIn("algorithm=retractable_ordered_multiset", dag.snapshot())
        dag.deduplicate("dedup", input="changes", workflow="Unused")
        with self.assertRaisesRegex(ValueError, "append-only"):
            dag.validate()


class CollectionIntegrationTests(unittest.TestCase):
    def setUp(self):
        if not SERVER.is_file():
            self.skipTest("build highwater-server to run integration tests")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.log = (Path(self.temp.name) / "server.log").open("w+")
        self.addCleanup(self.log.close)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.client = Client(f"http://127.0.0.1:{self.port}")
        self.server = None
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        self.server = subprocess.Popen([
            str(SERVER), "--listen", f"127.0.0.1:{self.port}",
            "--state-dir", self.temp.name + "/state", "--object-store-dir", self.temp.name + "/objects",
        ], stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                self.log.seek(0)
                self.fail(self.log.read())
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=.1):
                    return
            except OSError:
                time.sleep(.025)
        self.fail("server startup timed out")

    def stop(self):
        if self.server and self.server.poll() is None:
            self.server.terminate()
            self.server.wait(timeout=10)

    async def wait_rows(self, operator, values):
        deadline = time.monotonic() + 5
        while True:
            rows = await self.client.collection_rows(operator)
            actual = sorted((r["value"]["id"], r["value"]["score"], r["count"]) for r in rows)
            if actual == sorted(values):
                return rows
            if time.monotonic() > deadline:
                self.fail(f"{operator}: {actual} != {values}")
            await asyncio.sleep(.03)

    def test_upsert_group_movement_loser_promotion_backfill_and_restart(self):
        async def run():
            c = self.client
            await c.create_stream("scores", options=MANAGED)
            # Backfill must use admission sequence across partitions and preserve all losers.
            for i, score in enumerate([100, 90, 80]):
                await c.publish_event("scores", {"id": str(i), "league": "a", "score": score}, event_time=10+i, kind="upsert", event_id=f"seed-{i}")
            dag = Dag("leaders").stream("scores", MANAGED).stream("winner_changes", MANAGED)
            dag.top_n("leaders", input="scores", primary_key=["id"], partition_by=["league"],
                      order_by=[("score", "desc"), ("id", "asc")], n=2,
                      input_mode=CollectionMode.UPSERT, output="winner_changes")
            await dag.deploy(c)
            await dag.deploy(c)
            self.assertEqual(len(await c.collection_rows("leaders", group=["a"])), 2)
            # A native consumer runs with no workflow worker.
            await c.top_n("downstream", input="winner_changes", order_by=[("score", "desc")], n=1)
            await self.wait_rows("downstream", [("0", 100, 1)])
            snapshot = await c.snapshot_views("leaders")
            await c.publish_event("scores", {"id": "0"}, event_time=40, kind="delete", event_id="delete-zero")
            await self.wait_rows("leaders", [("1", 90, 1), ("2", 80, 1)])
            await self.wait_rows("downstream", [("1", 90, 1)])
            # Retry is transport deduplication, not another delete.
            duplicate = await c.publish_event("scores", {"id": "0"}, event_time=40, kind="delete", event_id="delete-zero")
            self.assertEqual(duplicate["disposition"], "duplicate")
            await c.publish_event("scores", {"id": "1", "league": "b", "score": 70}, event_time=50, kind="upsert")
            await self.wait_rows("leaders", [("1", 70, 1), ("2", 80, 1)])
            self.assertEqual([r["value"]["id"] for r in await c.collection_rows("leaders", group=["a"])], ["2"])
            changes = await c.read_operator_changes("leaders")
            deleted = [r for r in changes if r["kind"] == "delete" and r["row"]["id"] == "0"]
            self.assertEqual(deleted[0]["event_time"], 10)
            self.assertIsNone((await c.stream_info("winner_changes")).watermark)
            self.stop()
            self.start()
            await self.wait_rows("leaders", [("1", 70, 1), ("2", 80, 1)])
            await self.wait_rows("downstream", [("2", 80, 1)])
            restored = await c.read_view_snapshot(snapshot.snapshot_id)
            self.assertEqual(len(restored.rows("leaders")), 2)
            await c.delete_view_snapshot(snapshot.snapshot_id)
            self.assertEqual((await c.relational_operator("leaders"))["algorithm"], "retractable_ordered_multiset")
        asyncio.run(run())

    def test_normalize_filter_top_n_window_and_atomic_rejection(self):
        async def run():
            c = self.client
            for stream in ("raw", "normalized", "positive", "winners"):
                await c.create_stream(stream, options=MANAGED)
            await c.normalize("normalize", input="raw", primary_key=["id"])
            await c.connect_operator("normalize", "normalized")
            await c._request("POST", "/stream-filters", {"operator_id": "positive", "stream": "normalized",
                "workflow_type": "Unused", "field": "score", "comparison": "greater_than", "operand": 0})
            await c.connect_operator("positive", "positive")
            await c.top_n("top", input="positive", order_by=[("score", "desc")], n=1)
            await c.connect_operator("top", "winners")
            await c._request("POST", "/stream-schedules", {"schedule_id": "count", "stream": "winners",
                "workflow_type": "Unused", "window_size": 10, "start_at": 0, "aggregation": "count"})
            await c.publish_event("raw", {"id": "a", "score": 9}, key="key", event_time=1, kind="upsert")
            await self.wait_rows("top", [("a", 9, 1)])
            # Replacement crosses the filter boundary at a later time/window.
            await c.publish_event("raw", {"id": "a", "score": -1}, key="key", event_time=21, kind="upsert")
            await self.wait_rows("top", [])
            deadline = time.monotonic() + 5
            while True:
                result = await c.view("count").get("[]")
                if not result:
                    break
                if time.monotonic() > deadline:
                    self.fail("window did not retract original-time row")
                await asyncio.sleep(.03)
            # Normalizer can delete by primary key alone.
            await c.publish_event("raw", {"id": "a"}, event_time=30, kind="delete")
            await self.wait_rows("normalize", [])
            before = (await c.stream_info("raw")).partitions[0]["next_offset"]
            with self.assertRaisesRegex(RuntimeError, "unknown primary key"):
                await c.publish_events("raw", [
                    {"event_time": 40, "kind": "upsert", "value": {"id": "b", "score": 4}},
                    {"event_time": 41, "kind": "delete", "value": {"id": "missing"}},
                ])
            self.assertEqual((await c.stream_info("raw")).partitions[0]["next_offset"], before)
            await self.wait_rows("normalize", [])
            # New collections cannot be attached to a consumer that cannot retract.
            with self.assertRaisesRegex(RuntimeError, "append-only"):
                await c._request("POST", "/deduplicates", {"operator_id": "bad", "stream": "positive", "workflow_type": "Unused"})
            with self.assertRaisesRegex(RuntimeError, "already used"):
                await c._request("POST", "/stream-filters", {"operator_id": "top", "stream": "raw", "workflow_type": "Unused",
                    "field": "score", "comparison": "greater_than", "operand": 0})
            with self.assertRaisesRegex(RuntimeError, "retract mode"):
                await c.top_n("bad-mode", input="normalized", order_by=[("score", "desc")], n=1, input_mode=CollectionMode.APPEND)
        asyncio.run(run())


    def test_process_corrections_normalize_time_before_interval_join(self):
        async def run():
            c = self.client
            for stream in ("events", "process_changes", "corrected", "right"):
                await c.create_stream(stream, options=MANAGED)
            handle = await c.start(CurrentScore, source="events", process_id="scores-process")
            await c.connect_operator("scores-process", "process_changes")
            await c.normalize("correct", input="process_changes", primary_key=["id"], input_mode=CollectionMode.RETRACT)
            await c.connect_operator("correct", "corrected")
            await c._request("POST", "/interval-joins", {"join_id": "pairs", "left_stream": "corrected", "right_stream": "right",
                "workflow_type": "Unused", "task_queue": "unused", "lower_bound": 0, "upper_bound": 0})
            await c.publish_event("right", {"match": True}, key="a", event_time=1, kind="insert")
            registry = Registry()
            registry.register_workflow(CurrentScore)
            worker = asyncio.create_task(RustWorker(registry, target=c.target, task_queue="default").run_forever())
            try:
                await handle.send({"id": "a", "score": 9}, event_time=1, event_id="first")
                await handle.drain(timeout=10)
                deadline = time.monotonic() + 5
                while not await c.view("pairs").get("a"):
                    if time.monotonic() > deadline:
                        self.fail("initial interval pair not produced")
                    await asyncio.sleep(.03)
                await handle.send({"id": "a", "score": 4}, event_time=21, event_id="second")
                await handle.drain(timeout=10)
                await self.wait_rows("correct", [("a", 4, 1)])
                deadline = time.monotonic() + 5
                while await c.view("pairs").get("a"):
                    if time.monotonic() > deadline:
                        self.fail("Process correction did not retract original interval pair")
                    await asyncio.sleep(.03)
                changes = await c.read_operator_changes("correct")
                self.assertEqual([r["event_time"] for r in changes if r["kind"] == "delete"], [1])
            finally:
                worker.cancel()
                outcome, = await asyncio.gather(worker, return_exceptions=True)
                if isinstance(outcome, Exception):
                    raise outcome
        asyncio.run(run())
