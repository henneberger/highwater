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

from highwater import (
    AutoscalingPolicy,
    Client,
    Registry,
    WorkloadSample,
    assign_partitions,
    recommend_replicas,
    streaming,
)
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


@streaming.process(key="key", build_id="elastic-slow-counter-v1")
@dataclass
class SlowElasticCounter:
    total: int = 0

    @streaming.event
    async def increment(self, event):
        await asyncio.sleep(0.001)
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

    def test_zero_worker_pool_wakes_for_admitted_work(self) -> None:
        async def run() -> None:
            client = Client(self.target, poll_interval=0.001)
            counters = await client.start(
                ElasticCounter,
                process_id="scale-to-zero-counters",
            )
            before_info = await counters.info()
            before = WorkloadSample(
                time.time(),
                before_info["pending"],
                before_info["running"],
                before_info["completed"],
            )
            await counters.send_many([
                {"key": f"wake-{index}", "amount": 1}
                for index in range(200)
            ])
            queued = await counters.info()
            self.assertEqual(queued["completed"], 0)
            await asyncio.sleep(0.01)
            decision = recommend_replicas(
                before,
                WorkloadSample(
                    time.time(),
                    queued["pending"],
                    queued["running"],
                    queued["completed"],
                ),
                current_replicas=0,
                partitions=4,
                policy=AutoscalingPolicy(
                    min_replicas=0,
                    max_replicas=4,
                    target_events_per_second_per_replica=100,
                    target_backlog_per_replica=100,
                    headroom=1,
                    scale_down_after=0,
                ),
            )
            self.assertGreater(decision.desired_replicas, 0)
            async with self.workers(decision.desired_replicas):
                await counters.drain(timeout=20)
            states = await asyncio.gather(*(
                counters.state(f"wake-{index}") for index in range(200)
            ))
            self.assertTrue(all(state == {"total": 1} for state in states))

        asyncio.run(run())

    def test_backlog_drives_live_scale_out_and_survives_worker_loss(self) -> None:
        async def run() -> None:
            client = Client(self.target, poll_interval=0.001)
            counters = await client.start(
                SlowElasticCounter,
                process_id="live-elastic-counters",
            )
            registry = Registry()
            registry.register_workflow(SlowElasticCounter)

            def launch(assignments):
                return [
                    asyncio.create_task(RustWorker(
                        registry,
                        target=self.target,
                        process_partitions=assignment,
                        process_only=True,
                        lease_seconds=1,
                    ).run_forever())
                    for assignment in assignments
                ]

            initial = launch(((1, 2, 3, 4),))
            started = time.time()
            before_info = await counters.info()
            before = WorkloadSample(
                started,
                before_info["pending"],
                before_info["running"],
                before_info["completed"],
            )

            async def publish_round(amount: int) -> None:
                for offset in range(0, 2_000, 100):
                    await counters.send_many([
                        {"key": f"key-{index}", "amount": amount}
                        for index in range(offset, offset + 100)
                    ])
                    await asyncio.sleep(0.005)

            first_publish = asyncio.create_task(publish_round(1))
            await asyncio.sleep(0.04)
            during_info = await counters.info()
            decision = recommend_replicas(
                before,
                WorkloadSample(
                    time.time(),
                    during_info["pending"],
                    during_info["running"],
                    during_info["completed"],
                ),
                current_replicas=1,
                partitions=4,
                policy=AutoscalingPolicy(
                    min_replicas=1,
                    max_replicas=4,
                    target_events_per_second_per_replica=100,
                    target_backlog_per_replica=100,
                    headroom=1,
                    scale_down_after=0,
                ),
            )
            self.assertFalse(first_publish.done())
            self.assertEqual(decision.desired_replicas, 4)

            scaled = launch(decision.partition_assignments)
            await asyncio.sleep(0.02)
            for task in initial:
                task.cancel()
            await asyncio.gather(*initial, return_exceptions=True)

            scaled[0].cancel()
            await asyncio.gather(scaled[0], return_exceptions=True)
            scaled[0] = launch((decision.partition_assignments[0],))[0]

            await first_publish
            second_publish = asyncio.create_task(publish_round(1))
            await second_publish
            await counters.drain(timeout=30)

            states = await asyncio.gather(*(
                counters.state(f"key-{index}") for index in range(2_000)
            ))
            self.assertTrue(all(state == {"total": 2} for state in states))
            process = await counters.info()
            self.assertEqual(process["completed"], 4_000)
            self.assertEqual(process["pending"], 0)
            self.assertEqual(process["running"], 0)

            for task in scaled:
                task.cancel()
            await asyncio.gather(*scaled, return_exceptions=True)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
