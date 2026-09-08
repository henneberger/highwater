from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest

from highwater import Client, Registry, streaming
from highwater.rust_worker import RustWorker


SERVER = Path(__file__).resolve().parents[1] / "target" / "debug" / "highwater-server"


@streaming.process(key="account_id", build_id="finality-account-v1")
@dataclass
class FinalityAccount:
    balance: int = 0

    @streaming.event
    async def apply(self, event):
        self.balance += event.amount
        return {"balance": self.balance}


class ProcessFinalityTest(unittest.TestCase):
    def setUp(self):
        if not SERVER.is_file():
            self.skipTest("build highwater-server to run integration tests")
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = (self.root / "server.log").open("w+")
        self.addCleanup(self.log.close)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.target = f"http://127.0.0.1:{self.port}"
        self.server = None
        self.addCleanup(self.stop_server)
        self.start_server()

    def start_server(self):
        self.server = subprocess.Popen([
            str(SERVER), "--listen", f"127.0.0.1:{self.port}",
            "--state-dir", str(self.root / "state"),
            "--object-store-dir", str(self.root / "objects"),
        ], stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                self.fail("server exited during startup")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        self.fail("server did not start")

    def stop_server(self):
        if self.server is not None and self.server.poll() is None:
            self.server.terminate()
            self.server.wait(timeout=10)

    def test_key_finality_rejects_pending_and_new_events_and_survives_restart(self):
        async def run():
            client = Client(self.target, poll_interval=0.01)
            handle = await client.start(FinalityAccount, process_id="finality-account")
            self.assertTrue(handle.direct_ingress)
            event = {"account_id": "closed", "amount": 7}
            accepted = await handle.send(event, event_time=10, event_id="first")
            with self.assertRaisesRegex(RuntimeError, "pending, running, or retrying"):
                await handle.finalize("closed")

            registry = Registry()
            registry.register_workflow(FinalityAccount)
            worker = RustWorker(registry, target=self.target)
            task = asyncio.create_task(worker.run_forever())
            try:
                await handle.drain(timeout=10)
                marker = await handle.finalize("closed")
                self.assertEqual(marker["basis"], "business_declaration")
                self.assertEqual(marker["key"], "closed")
                self.assertIsNotNone(marker["input_sequence"])
                self.assertEqual(await handle.finalize("closed"), marker)
                repeated = await handle.send(event, event_time=10, event_id="first")
                self.assertEqual(repeated["record"]["event_id"], accepted["record"]["event_id"])
                self.assertEqual(repeated["disposition"], "duplicate")
                with self.assertRaisesRegex(RuntimeError, "process key is finalized"):
                    await handle.send(event, event_time=11, event_id="second")
                self.assertEqual(await handle.state("closed"), {"balance": 7})

                # Closing one key leaves other keys available.
                await handle.send({"account_id": "open", "amount": 3}, event_id="other")
                await handle.finish(timeout=10)
                open_marker = await handle.finalize("open")
                self.assertEqual(await handle.finalize("open"), open_marker)
                await handle.finish(timeout=10)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            self.stop_server()
            self.start_server()
            self.assertEqual(await handle.finalize("closed"), marker)
            self.assertEqual(await handle.finality("closed"), marker)
            self.assertEqual(await handle.state("closed"), {"balance": 7})
            with self.assertRaisesRegex(RuntimeError, "process key is finalized"):
                await handle.send(event, event_time=12, event_id="after-restart")

        asyncio.run(run())

    def test_shared_stream_process_cannot_be_business_finalized(self):
        async def run():
            client = Client(self.target)
            await client.create_stream("shared-input")
            handle = await client.start(
                FinalityAccount, source="shared-input", process_id="shared-finality",
            )
            with self.assertRaisesRegex(RuntimeError, "direct-ingress process"):
                await handle.finalize("closed")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
