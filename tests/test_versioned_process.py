from __future__ import annotations

import asyncio
import socket
import subprocess
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

from examples.shopping_assistant import ShoppingAssistant
import examples.ai_support_operations as ai_support
from highwater import (
    Client,
    Comparison,
    Registry,
    StreamOptions,
    WatermarkMode,
    streaming,
)
from highwater.model import FilterSpec, ProcessSpec, TemporalJoinSpec
from highwater.rust_worker import RustWorker


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "highwater-server"


@streaming.process(
    name="UpgradeAccount",
    key="account_id",
    state_version=1,
    build_id="upgrade-account-v1",
)
@dataclass
class UpgradeAccountV1:
    balance: int = 0

    @streaming.event
    async def apply(self, event):
        self.balance += event.amount
        return {"balance": self.balance}


@streaming.process(
    name="UpgradeAccount",
    key="account_id",
    state_version=2,
    build_id="upgrade-account-v2",
)
@dataclass
class UpgradeAccountV2:
    balance: int = 0
    currency: str = "USD"

    @streaming.migrate(from_version=1)
    def migrate_v1(self, state):
        return {**state, "currency": "USD"}

    @streaming.event
    async def apply(self, event):
        self.balance += event.amount
        return {"balance": self.balance, "currency": self.currency}


batch_catalog = streaming.versioned("batch-catalog", key="product_id")


@streaming.process(key="session_id", event_time="event_time", build_id="batch-catalog-v1")
class BatchCatalogProcess:
    @streaming.batch(max_size=64, max_delay=0.002)
    async def apply(self, events, contexts):
        transitions = []
        for event, context in zip(events, contexts, strict=True):
            product = await batch_catalog.get(
                event.product_id, as_of=context.event_time,
            )
            views = (context.state or {}).get("views", 0) + 1
            transitions.append(streaming.transition(
                state={"category": product.category, "views": views},
                emit=(
                    {"product_id": event.product_id, "category": product.category}
                    if views % 5 == 0
                    else None
                ),
            ))
        return transitions


class VersionedProcessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not SERVER.is_file():
            raise unittest.SkipTest("build highwater-server to run integration tests")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            cls.port = listener.getsockname()[1]
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(cls.temporary.name)
        cls.server_log_path = root / "server.log"
        cls.server_log = cls.server_log_path.open("w+")
        cls.target = f"http://127.0.0.1:{cls.port}"
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
            except Exception:
                if cls.server.poll() is not None:
                    raise RuntimeError("Highwater server exited during startup")
                time.sleep(0.05)
            else:
                break
        else:
            raise RuntimeError("Highwater server did not start")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.terminate()
        cls.server.wait(timeout=10)
        cls.server_log.close()
        cls.temporary.cleanup()

    def test_process_waits_for_version_frontier_and_reads_as_of_event_time(self) -> None:
        async def run() -> None:
            client = Client(self.target)
            await client.create_stream(
                "catalog",
                options=StreamOptions(max_out_of_orderness=100, allowed_lateness=0),
            )
            handle = await client.start(
                ShoppingAssistant,
                process_id="shopping-assistant",
            )
            registry = Registry()
            registry.register_workflow(ShoppingAssistant)
            worker = RustWorker(registry, target=self.target)
            worker_task = asyncio.create_task(worker.run_forever())
            try:
                await client.publish_event(
                    "catalog",
                    {"product_id": "product-1", "category": "books"},
                    key="product-1",
                    event_time=5,
                )
                await client.publish_event(
                    "catalog",
                    {"product_id": "product-1", "category": "games"},
                    key="product-1",
                    event_time=15,
                )
                await handle.send(
                    {"user_id": "user-1", "product_id": "product-1"},
                    event_time=10,
                )
                self.assertEqual((await handle.info())["pending"], 1)

                await client.advance_watermark("catalog", 0, 10)
                await handle.drain(timeout=10)
                self.assertEqual(
                    await handle.state("user-1"),
                    {"recent": ["books"]},
                )

                await handle.send(
                    {"user_id": "user-1", "product_id": "product-1"},
                    event_time=20,
                )
                self.assertEqual((await handle.info())["pending"], 1)

                await client.advance_watermark("catalog", 0, 20)
                await handle.drain(timeout=10)
                self.assertEqual(
                    await handle.state("user-1"),
                    {"recent": ["books", "games"]},
                )
                positive_outputs = [
                    change["row"]
                    for change in await client.read_operator_changes(
                        "shopping-assistant"
                    )
                    if change["diff"] > 0
                ]
                self.assertEqual(positive_outputs, [
                    {
                        "product_id": "product-1",
                        "category": "books",
                        "recent": ["books"],
                    },
                    {
                        "product_id": "product-1",
                        "category": "games",
                        "recent": ["books", "games"],
                    },
                ])
                comparison = await client.compare_builds(
                    "shopping-assistant",
                    baseline=ShoppingAssistant,
                    candidate=ShoppingAssistant,
                )
                self.assertTrue(comparison.matches)
                self.assertEqual(comparison.events, 2)
            finally:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)

        asyncio.run(run())

    def test_batch_process_resolves_multiple_versioned_keys(self) -> None:
        async def run() -> None:
            client = Client(self.target, poll_interval=0.01)
            await client.create_stream(
                "batch-catalog",
                options=StreamOptions(max_out_of_orderness=0, allowed_lateness=0),
            )
            for index in range(3):
                await client.publish_event(
                    "batch-catalog",
                    {"product_id": f"product-{index}", "category": f"category-{index}"},
                    key=f"product-{index}",
                    event_time=index + 1,
                    event_id=f"batch-catalog-{index}",
                )
            await client.advance_watermark("batch-catalog", 0, 10)
            handle = await client.start(
                BatchCatalogProcess,
                process_id="batch-catalog-process",
            )
            registry = Registry()
            registry.register_workflow(BatchCatalogProcess)
            worker_tasks = {
                asyncio.create_task(RustWorker(
                    registry,
                    target=self.target,
                    process_partitions=(partition,),
                ).run_forever())
                for partition in range(1, 5)
            }
            try:
                await handle.send_many([
                    {
                        "session_id": f"session-{index % 4}",
                        "product_id": f"product-{index % 3}",
                        "event_time": 5,
                    }
                    for index in range(20)
                ])
                drain = asyncio.create_task(handle.drain(timeout=10))
                done, _ = await asyncio.wait(
                    {drain, *worker_tasks}, return_when=asyncio.FIRST_COMPLETED,
                )
                for worker_task in done & worker_tasks:
                    worker_task.result()
                await drain
                self.assertEqual((await handle.info())["completed"], 20)
                self.assertEqual(await handle.state("session-1"), {
                    "category": "category-2", "views": 5,
                })
            finally:
                for worker_task in worker_tasks:
                    worker_task.cancel()
                await asyncio.gather(*worker_tasks, return_exceptions=True)

        asyncio.run(run())

    def test_process_upgrade_migrates_live_keyed_state(self) -> None:
        async def run() -> None:
            client = Client(self.target)
            original = await client.start(
                UpgradeAccountV1,
                process_id="upgrade-accounts",
            )
            registry_v1 = Registry()
            registry_v1.register_workflow(UpgradeAccountV1)
            worker_v1 = RustWorker(registry_v1, target=self.target)
            worker_v1_task = asyncio.create_task(worker_v1.run_forever())
            try:
                await original.send({"account_id": "a", "amount": 5})
                await original.drain(timeout=10)
            finally:
                worker_v1_task.cancel()
                await asyncio.gather(worker_v1_task, return_exceptions=True)

            upgraded = await client.start(
                UpgradeAccountV2,
                source=original.input,
                process_id="upgrade-accounts",
            )
            registry_v2 = Registry()
            registry_v2.register_workflow(UpgradeAccountV2)
            worker_v2 = RustWorker(registry_v2, target=self.target)
            worker_v2_task = asyncio.create_task(worker_v2.run_forever())
            try:
                await upgraded.send({"account_id": "a", "amount": 7})
                await upgraded.drain(timeout=10)
                self.assertEqual(
                    await upgraded.state("a"),
                    {"balance": 12, "currency": "USD"},
                )
                process = await upgraded.info()
                self.assertEqual(process["state_version"], 2)
                self.assertEqual(
                    process["active_build_id"],
                    "upgrade-account-v2",
                )
            finally:
                worker_v2_task.cancel()
                await asyncio.gather(worker_v2_task, return_exceptions=True)

        asyncio.run(run())

    def test_complex_ai_dag_deploys_and_executes(self) -> None:
        async def run() -> None:
            client = Client(self.target)
            await ai_support.topology().deploy(client)
            await ai_support.topology().deploy(client)

            triage = await client.process("support-triage")
            self.assertEqual(
                triage["versioned_streams"],
                ["account-policy-versions", "support-knowledge-versions"],
            )
            self.assertEqual(triage["state_version"], 2)

            route = await client.temporal_join("route-escalations")
            self.assertEqual(route["probe_stream"], "escalations")
            self.assertEqual(route["version_stream"], "service-plan-versions")

            feedback = await client.interval_join("match-decisions-to-feedback")
            self.assertEqual(feedback["lower_bound"], 0)
            self.assertEqual(feedback["upper_bound"], 604_800)

            usage = await client.window_schedule("aggregate-hourly-token-usage")
            self.assertEqual(usage["window_size"], 3600)
            self.assertEqual(usage["slide"], 300)
            self.assertEqual(usage["value_field"], "tokens")

            for operator_id, output in (
                ("deduplicate-support-events", "unique-support-events"),
                ("support-triage", "agent-decisions"),
                ("select-escalations", "escalations"),
                ("route-escalations", "routed-escalations"),
                ("coordinate-handoffs", "human-handoffs"),
                ("page-high-priority", "pager-events"),
                ("aggregate-hourly-token-usage", "hourly-token-usage"),
                ("match-decisions-to-feedback", "resolution-outcomes"),
                ("aggregate-daily-feedback-scores", "daily-feedback-scores"),
            ):
                edge = await client.operator_edge(operator_id)
                self.assertEqual(edge["output_stream"], output)

            registry = Registry().discover(ai_support)
            worker = RustWorker(registry, target=self.target)
            worker_task = asyncio.create_task(worker.run_forever())

            async def records(stream: str) -> list:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    current = await client.read_stream(stream)
                    if current:
                        return current
                    if worker_task.done():
                        worker_task.result()
                    await asyncio.sleep(0.05)
                diagnostics = {
                    name: len(await client.read_stream(name))
                    for name in (
                        "support-events",
                        "unique-support-events",
                        "agent-decisions",
                        "escalations",
                        "routed-escalations",
                        "human-handoffs",
                    )
                }
                diagnostics["triage"] = await client.process("support-triage")
                diagnostics["deduplicate"] = await client.deduplicate(
                    "deduplicate-support-events"
                )
                diagnostics["deduplicate_edge"] = await client.operator_edge(
                    "deduplicate-support-events"
                )
                diagnostics["deduplicate_changes"] = await client.read_operator_changes(
                    "deduplicate-support-events"
                )
                self.server_log.flush()
                diagnostics["server_log"] = self.server_log_path.read_text()
                raise TimeoutError(f"no records reached {stream}: {diagnostics}")

            try:
                await client.publish_event(
                    "account-policy-versions",
                    {"priority_support": True},
                    key="customer-1",
                    event_time=0,
                )
                await client.publish_event(
                    "support-knowledge-versions",
                    {"title": "Reset a password"},
                    key="password-reset",
                    event_time=0,
                )
                await client.publish_event(
                    "service-plan-versions",
                    {"queue": "priority-ai-support"},
                    key="customer-1",
                    event_time=0,
                )
                await client.publish_event(
                    "support-events",
                    {
                        "case_id": "case-1",
                        "customer_id": "customer-1",
                        "topic": "unexpected-agent-action",
                        "text": "The assistant changed a setting I did not ask for.",
                        "occurred_at": 10,
                    },
                    key="customer-1",
                    event_time=10,
                    event_id="case-1:message-1",
                )
                await client.publish_event(
                    "customer-feedback",
                    {"case_id": "case-1", "score": 2},
                    key="customer-1",
                    event_time=20,
                    event_id="case-1:feedback",
                )

                for stream in (
                    "account-policy-versions",
                    "support-knowledge-versions",
                    "service-plan-versions",
                ):
                    await client.advance_watermark(stream, 0, 100_000)
                await client.advance_watermark("support-events", 0, 100_000)
                await client.advance_watermark("customer-feedback", 0, 100_000)

                decisions = await records("agent-decisions")
                self.assertEqual(decisions[0].value["case_id"], "case-1")
                self.assertTrue(decisions[0].value["requires_human"])
                self.assertEqual(decisions[0].value["priority"], 3)

                handoffs = await records("human-handoffs")
                self.assertEqual(handoffs[0].value["queue"], "priority-ai-support")
                self.assertEqual(handoffs[0].value["case_id"], "case-1")

                await records("pager-events")
                await records("resolution-outcomes")
                await records("hourly-token-usage")
                await records("daily-feedback-scores")
            finally:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)

        asyncio.run(run())

    def test_runtime_cycle_detection_includes_all_temporal_dependencies(self) -> None:
        async def run() -> None:
            client = Client(self.target)
            source_managed = StreamOptions(
                watermark_mode=WatermarkMode.SOURCE_MANAGED,
            )
            for stream in (
                "cycle-process-events",
                "cycle-process-reference",
                "cycle-temporal-probes",
                "cycle-temporal-versions",
                "cycle-update-events",
                "cycle-update-output",
                "cycle-update-reference",
            ):
                await client.create_stream(stream, options=source_managed)

            await client._deploy_operator(ProcessSpec(
                process_id="cycle-process",
                input="cycle-process-events",
                workflow="CycleProcess",
                build_id="cycle-process-v1",
                versioned_streams=("cycle-process-reference",),
            ))
            with self.assertRaisesRegex(RuntimeError, "would create a cycle"):
                await client.connect_operator(
                    "cycle-process",
                    "cycle-process-reference",
                )

            await client._deploy_operator(TemporalJoinSpec(
                operator_id="cycle-temporal-join",
                probe_stream="cycle-temporal-probes",
                version_stream="cycle-temporal-versions",
                workflow="CycleTemporalJoin",
            ))
            with self.assertRaisesRegex(RuntimeError, "would create a cycle"):
                await client.connect_operator(
                    "cycle-temporal-join",
                    "cycle-temporal-probes",
                )

            await client._deploy_operator(ProcessSpec(
                process_id="cycle-update-process",
                input="cycle-update-events",
                workflow="CycleUpdateProcess",
                build_id="cycle-update-v1",
            ))
            await client.connect_operator(
                "cycle-update-process",
                "cycle-update-output",
            )
            await client._deploy_operator(FilterSpec(
                operator_id="cycle-update-filter",
                stream="cycle-update-output",
                workflow="CycleUpdateFilter",
                field="selected",
                comparison=Comparison.EQUAL,
                operand=True,
            ))
            await client.connect_operator(
                "cycle-update-filter",
                "cycle-update-reference",
            )
            with self.assertRaisesRegex(RuntimeError, "would create.*cycle"):
                await client._deploy_operator(ProcessSpec(
                    process_id="cycle-update-process",
                    input="cycle-update-events",
                    workflow="CycleUpdateProcess",
                    build_id="cycle-update-v2",
                    versioned_streams=("cycle-update-reference",),
                ))

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
