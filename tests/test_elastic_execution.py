from __future__ import annotations

import asyncio
import socket
import subprocess
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from highwater import Client, Registry, assign_partitions, streaming
from highwater.rust_worker import RustWorker


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "highwater-server"


@streaming.process(key="key", build_id="elastic-counter-v1")
@dataclass
class ElasticCounter:
    total: int = 0

    @streaming.event
    async def increment(self, event):
        self.total += event.amount
        return {"key": event.key, "total": self.total}


class ElasticExecutionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not SERVER.is_file():
            raise unittest.SkipTest("build highwater-server to run elastic execution test")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            cls.port = listener.getsockname()[1]
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(cls.temporary.name)
        cls.target = f"http://127.0.0.1:{cls.port}"
        cls.server = subprocess.Popen(
            [
                str(SERVER),
                "--listen", f"127.0.0.1:{cls.port}",
                "--state-dir", str(root / "state"),
                "--object-store-dir", str(root / "objects"),
                "--log-shards", "5",
                "--key-groups", "128",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.1):
                    break
            except OSError:
                if cls.server.poll() is not None:
                    raise RuntimeError("elastic execution server stopped during startup")
                time.sleep(0.05)
        else:
            raise RuntimeError("elastic execution server did not start")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.terminate()
        cls.server.wait(timeout=10)
        cls.temporary.cleanup()

    @asynccontextmanager
    async def workers(self, replicas: int):
        registry = Registry()
        registry.register_workflow(ElasticCounter)
        assignments = assign_partitions(4, replicas)
        tasks = [
            asyncio.create_task(RustWorker(
                registry,
                target=self.target,
                process_partitions=assignment,
                process_only=True,
            ).run_forever())
            for assignment in assignments
        ]
        try:
            yield assignments
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def test_scale_out_preserves_every_key_and_ordered_state(self) -> None:
        async def run() -> None:
            client = Client(self.target, poll_interval=0.001)
            counters = await client.start(
                ElasticCounter,
                process_id="elastic-counters",
            )
            first = [{"key": f"key-{index}", "amount": 1} for index in range(200)]
            async with self.workers(2) as assignments:
                self.assertEqual(assignments, ((1, 3), (2, 4)))
                await counters.send_many(first)
                await counters.drain(timeout=20)

            second = [{"key": f"key-{index}", "amount": 1} for index in range(200)]
            async with self.workers(4) as assignments:
                self.assertEqual(assignments, ((1,), (2,), (3,), (4,)))
                await counters.send_many(second)
                await counters.drain(timeout=20)

            states = await asyncio.gather(*(
                counters.state(f"key-{index}") for index in range(200)
            ))
            self.assertTrue(all(state == {"total": 2} for state in states))
            process = await counters.info()
            self.assertEqual(process["completed"], 400)
            self.assertEqual(process["pending"], 0)
            self.assertEqual(process["running"], 0)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
