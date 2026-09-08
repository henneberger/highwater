from __future__ import annotations

import asyncio
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from highwater import Client, ChangeKind, StreamOptions

SERVER = Path(__file__).resolve().parents[1] / "target/debug/highwater-server"


class ViewSnapshotTests(unittest.TestCase):
    def test_retractions_shared_cut_and_durable_snapshot(self):
        if not SERVER.exists():
            self.skipTest("build highwater-server first")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            target = f"http://127.0.0.1:{port}"
            command = [str(SERVER), "--listen", f"127.0.0.1:{port}",
                       "--state-dir", str(root / "state"),
                       "--object-store-dir", str(root / "objects")]
            with (root / "server.log").open("w+") as log:
                def start():
                    server = subprocess.Popen(command, stdout=log, stderr=log)
                    for _ in range(200):
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=.1):
                                return server
                        except OSError:
                            if server.poll() is not None:
                                log.seek(0)
                                self.fail(log.read())
                            time.sleep(.025)
                    server.terminate()
                    server.wait(timeout=10)
                    self.fail("server did not start")

                async def capture():
                    client = Client(target)
                    await client.create_stream("input", options=StreamOptions(max_out_of_orderness=100))
                    for name in ("a", "b"):
                        await client._request("POST", "/stream-filters", {
                            "operator_id": name, "stream": "input", "workflow_type": "Unused",
                            "task_queue": "default", "field": "amount", "comparison": "greater_than",
                            "operand": 0,
                        })
                    await client.publish_event("input", {"amount": 1}, key="k", event_time=1,
                                               kind=ChangeKind.INSERT)
                    before = await client.snapshot_views("a", "b")
                    self.assertEqual(before.consistency, "shared_input_cut")
                    self.assertEqual(before.rows("a"), before.rows("b"))
                    self.assertEqual(before.input_positions["input"][0]["next_offset"], 1)
                    await client.publish_events("input", [
                        {"partition": 0, "key": "k", "event_time": 2,
                         "kind": "update_before", "value": {"amount": 1}},
                        {"partition": 0, "key": "k", "event_time": 2,
                         "kind": "update_after", "value": {"amount": 2}},
                    ])
                    current = await client.view("a").get("k")
                    self.assertEqual([(row.value, row.count) for row in current], [({"amount": 2}, 1)])
                    saved = await client.read_view_snapshot(before.snapshot_id)
                    self.assertEqual(saved.rows("a")[0].value, {"amount": 1})
                    with self.assertRaises(RuntimeError):
                        await client.snapshot_views("a", "a")
                    await client.create_stream("other")
                    await client._request("POST", "/stream-filters", {
                        "operator_id": "other", "stream": "other", "workflow_type": "Unused",
                        "task_queue": "default", "field": "amount", "comparison": "greater_than", "operand": 0,
                    })
                    with self.assertRaises(RuntimeError):
                        await client.snapshot_views("a", "other")
                    return before.snapshot_id

                server = start()
                try:
                    snapshot_id = asyncio.run(capture())
                finally:
                    server.terminate()
                    server.wait(timeout=10)
                server = start()
                try:
                    async def restore():
                        client = Client(target)
                        saved = await client.read_view_snapshot(snapshot_id)
                        self.assertEqual(saved.rows("b")[0].value, {"amount": 1})
                        await client.delete_view_snapshot(snapshot_id)
                        with self.assertRaises(RuntimeError):
                            await client.read_view_snapshot(snapshot_id)
                    asyncio.run(restore())
                finally:
                    server.terminate()
                    server.wait(timeout=10)
