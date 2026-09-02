from __future__ import annotations

import asyncio
import io
import json
import socket
import subprocess
import tempfile
import time
import unittest
from contextlib import asynccontextmanager, redirect_stdout
from dataclasses import asdict
from pathlib import Path
from types import ModuleType

import examples.batched_embeddings as batched_embeddings
import examples.clickstream_recommendation as clickstream_recommendation
import examples.continuous_order_enrichment as continuous_order_enrichment
import examples.deduplicate as deduplicate
import examples.durable_order_pipeline as durable_order_pipeline
import examples.durable_process as durable_process
import examples.event_time_windows as event_time_windows
import examples.iot_sensor_metrics as iot_sensor_metrics
import examples.interval_join as interval_join
import examples.temporal_join as temporal_join
from highwater import Client, Registry
from highwater.rust_worker import RustWorker


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "highwater-server"


class ExampleDagExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not SERVER.is_file():
            raise unittest.SkipTest("build highwater-server to run integration tests")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            cls.port = listener.getsockname()[1]
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(cls.temporary.name)
        cls.target = f"http://127.0.0.1:{cls.port}"
        cls.server_log_path = root / "server.log"
        cls.server_log = cls.server_log_path.open("w+")
        cls.server = subprocess.Popen(
            [
                str(SERVER),
                "--listen", f"127.0.0.1:{cls.port}",
                "--state-dir", str(root / "state"),
                "--object-store-dir", str(root / "objects"),
            ],
            stdout=cls.server_log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.2):
                    break
            except OSError:
                if cls.server.poll() is not None:
                    raise RuntimeError("Highwater server exited during startup")
                time.sleep(0.05)
        else:
            raise RuntimeError("Highwater server did not start")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.terminate()
        cls.server.wait(timeout=10)
        cls.server_log.close()
        cls.temporary.cleanup()

    @asynccontextmanager
    async def worker(self, *modules: ModuleType):
        registry = Registry()
        for module in modules:
            registry.discover(module)
        worker = RustWorker(registry, target=self.target)
        task = asyncio.create_task(worker.run_forever())
        try:
            yield task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def run_example(self, module: ModuleType, timeout: float = 30) -> dict:
        output = io.StringIO()
        async with self.worker(module):
            with redirect_stdout(output):
                await asyncio.wait_for(module.main(self.target), timeout=timeout)
        return json.loads(output.getvalue())

    def test_batched_embeddings(self) -> None:
        result = asyncio.run(self.run_example(batched_embeddings))
        self.assertEqual(len(result["embeddings"]), 4)
        self.assertEqual(result["process"]["completed"], 4)
        rows = {change["row"]["document_id"]: change["row"] for change in result["embeddings"]}
        self.assertEqual(set(rows), {"doc-1", "doc-2", "doc-3", "doc-4"})
        burst_sizes = [
            rows[name]["batch_size"]
            for name in ("doc-1", "doc-2", "doc-3")
        ]
        self.assertTrue(all(1 <= size <= 3 for size in burst_sizes))
        for size in set(burst_sizes):
            self.assertEqual(burst_sizes.count(size) % size, 0)
        self.assertEqual(rows["doc-4"]["batch_size"], 1)
        self.assertTrue(all(len(row["embedding"]) == 8 for row in rows.values()))

    def test_process_native_temporal_lookup(self) -> None:
        async def run() -> list[dict]:
            output = io.StringIO()
            with redirect_stdout(output):
                await temporal_join.main(self.target)
            return json.loads(output.getvalue())

        rows = {
            row["purchase_id"]: row
            for row in asyncio.run(run())
        }
        self.assertEqual(rows["purchase-before-update"]["account_version"], "a-v1")
        self.assertEqual(rows["purchase-after-update"]["account_version"], "a-v2")
        self.assertTrue(rows["purchase-b"]["matched"])
        self.assertFalse(rows["purchase-low"]["matched"])
        self.assertEqual(
            rows["purchase-deleted"]["reason"],
            "no account version existed at purchase time",
        )

    def test_clickstream_recommendation(self) -> None:
        result = asyncio.run(self.run_example(clickstream_recommendation))
        recommendations = {
            (row["user_id"], row["url"], row["recommendation"], row["event_time"])
            for row in result["recommendation_updates"]
        }
        self.assertEqual(recommendations, {
            ("user-1", "/home", "/products", 120),
            ("user-1", "/home", "/checkout", 500),
            ("user-1", "/products", "/checkout", 500),
        })
        content = [
            (row["user_id"], row["url"], row["event_time"], row["content"]["title"])
            for row in result["content_at_click"]
        ]
        self.assertEqual(content, [
            ("user-1", "/home", 0, "Home"),
            ("user-1", "/products", 120, "Products"),
            ("user-2", "/home", 200, "Home"),
            ("user-1", "/checkout", 500, "Checkout"),
            ("user-2", "/products", 900, "Products"),
        ])

    def test_deduplicate(self) -> None:
        result = asyncio.run(self.run_example(deduplicate))
        self.assertEqual(result["emitted"], [
            {
                "event_time": 7.0,
                "key": "a",
                "value": {"command_id": "a-earliest-event-time"},
            },
            {
                "event_time": 9.0,
                "key": "b",
                "value": {"command_id": "b-canonical"},
            },
        ])
        self.assertEqual(result["suppressed"], [
            {"command_id": "a-arrived-first"},
            {"command_id": "a-later-duplicate"},
        ])

    def test_durable_process(self) -> None:
        result = asyncio.run(self.run_example(durable_process))
        self.assertEqual(result["account_a"], {
            "balance": 12,
            "events": 2,
            "complete_through": 12.0,
        })
        self.assertEqual(result["account_b"], {
            "balance": 3,
            "events": 1,
            "complete_through": 11.0,
        })
        self.assertEqual(result["process"]["completed"], 3)
        self.assertEqual(result["process"]["pending"], 0)
        balances_by_key = {
            key: [
                change["row"]["balance"]
                for change in result["state_changelog"]
                if change["key"] == key
            ]
            for key in ("account-a", "account-b")
        }
        self.assertEqual(balances_by_key, {
            "account-a": [5, 5, 12],
            "account-b": [3],
        })

    def test_event_time_windows(self) -> None:
        result = asyncio.run(self.run_example(event_time_windows))
        self.assertEqual([window["sum"] for window in result["windows"]], [7, 23])
        self.assertEqual(result["event_time_gate"]["watermark_reached"], 21)
        self.assertEqual(result["source_retry_disposition"], "duplicate")
        self.assertEqual(result["too_late_disposition"], "side_output")
        self.assertEqual(result["late_side_output"], [99])
        self.assertEqual(result["owner_epochs"], [1, 2])

    def test_iot_sensor_metrics(self) -> None:
        result = asyncio.run(self.run_example(iot_sensor_metrics))
        self.assertEqual(
            [
                (alert["sensor_id"], alert["temperature"], alert["event_time"])
                for alert in result["alerts"]
            ],
            [("sensor-a", 105, 15), ("sensor-b", 120, 18)],
        )
        self.assertEqual(result["durable_alert_changelog"], [
            {"sensor_id": "sensor-a", "temperature": 105},
            {"sensor_id": "sensor-b", "temperature": 120},
        ])
        self.assertEqual(
            [
                (
                    window["sensor_id"],
                    window["window_start"],
                    window["max_temperature"],
                    window["reading_count"],
                )
                for window in result["sliding_maxima"]
            ],
            [
                ("sensor-a", 0, 105, 3),
                ("sensor-b", 0, 120, 2),
                ("sensor-a", 10, 105, 2),
                ("sensor-b", 10, 120, 1),
            ],
        )

    def test_interval_join(self) -> None:
        result = asyncio.run(self.run_example(interval_join))
        self.assertEqual(
            [row["purchase_id"] for row in result["results"]],
            ["purchase-country", "purchase-low"],
        )
        self.assertEqual(
            [row["event_time_delta"] for row in result["results"]],
            [4.0, 3.0],
        )
        self.assertTrue(all(not row["matched"] for row in result["results"]))
        changes = result["changelog"]
        purchase_a = [
            change
            for change in changes
            if change["row"]["right"]["value"]["purchase_id"] == "purchase-a"
        ]
        self.assertEqual([change["diff"] for change in purchase_a], [1, -1])
        self.assertNotIn(
            "purchase-outside",
            {
                change["row"]["right"]["value"]["purchase_id"]
                for change in changes
            },
        )

    def test_continuous_order_enrichment(self) -> None:
        async def run() -> None:
            client = Client(self.target)
            await continuous_order_enrichment.deploy(client)
            async with self.worker(durable_order_pipeline):
                events = [
                    continuous_order_enrichment.event_for_offset(offset, 12)[0]
                    for offset in range(3)
                ]
                _, profile, customer_id, profile_time = (
                    continuous_order_enrichment.event_for_offset(0, 12)
                )
                await client.publish_event(
                    continuous_order_enrichment.CUSTOMER_PROFILES,
                    profile,
                    key=customer_id,
                    event_time=profile_time,
                    event_id=f"{customer_id}:profile",
                )
                for index, event in enumerate(events):
                    await client.publish_event(
                        continuous_order_enrichment.ORDER_EVENTS,
                        asdict(event),
                        key=customer_id,
                        event_time=event.occurred_at,
                        event_id=f"{event.order_id}:{index}",
                    )
                complete_through = events[-1].occurred_at + 1
                await client.advance_watermark(
                    continuous_order_enrichment.ORDER_EVENTS,
                    0,
                    complete_through,
                )
                await client.advance_watermark(
                    continuous_order_enrichment.CUSTOMER_PROFILES,
                    0,
                    complete_through,
                )

                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    outputs = await client.read_temporal_join(
                        continuous_order_enrichment.ORDER_ENRICHMENT
                    )
                    if outputs and outputs[0].workflow_id is not None:
                        result = await client.get_workflow_handle(
                            outputs[0].workflow_id
                        ).result(timeout=15)
                        self.assertEqual(result["status"], "shipped")
                        self.assertEqual(result["customer_version"], profile["version"])
                        self.assertEqual(result["charged"], 3_200)
                        self.assertEqual(result["attempts"], 2)
                        self.assertEqual(result["conditions"], {
                            "customer_active": True,
                            "within_order_limit": True,
                        })
                        return
                    await asyncio.sleep(0.05)
                raise TimeoutError("continuous order enrichment produced no joined output")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
