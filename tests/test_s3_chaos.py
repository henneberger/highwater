from __future__ import annotations

import json
import asyncio
import os
import shutil
import socket
import subprocess
import sys
import threading
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "release" / "highwater-server"
RUN_CHAOS = os.environ.get("HIGHWATER_S3_CHAOS") == "1"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 10,
) -> tuple[int, Any]:
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else None
    except urllib.error.HTTPError as error:
        try:
            payload = error.read()
            try:
                detail = json.loads(payload) if payload else None
            except json.JSONDecodeError:
                detail = payload.decode(errors="replace")
            return error.code, detail
        finally:
            error.close()


@unittest.skipUnless(
    RUN_CHAOS,
    "set HIGHWATER_S3_CHAOS=1 to run the multi-process S3 failure test",
)
class S3ChaosTest(unittest.TestCase):
    @unittest.skipIf(os.environ.get("HIGHWATER_S3_CHAOS_URI"), "TCP isolation drill uses local MinIO")
    def test_isolated_live_owner_cannot_commit_after_reconnection(self) -> None:
        from highwater import Client
        from highwater.client import ProcessHandle
        if __package__:
            from .network_faults import TCPFaultProxy
        else:
            from network_faults import TCPFaultProxy

        upstream = urllib.parse.urlparse(self.aws_env["AWS_ENDPOINT"])
        proxy = TCPFaultProxy((upstream.hostname, upstream.port))
        self.addCleanup(proxy.close)
        a_public, a_execution, b_public, b_execution = (free_port() for _ in range(4))
        node_a = self._start_node("net-a", a_public, a_execution, storage_endpoint=proxy.endpoint)
        self._wait_for_owners(a_public, "net-a")
        node_b = self._start_node("net-b", b_public, b_execution)
        self._wait_for_owners(b_public, "net-a")
        self.assertTrue(proxy.used.is_set(), "owner bypassed the storage proxy")
        status, detail = request_json("POST", f"http://127.0.0.1:{a_public}/streams", {"name": "network-input"})
        self.assertEqual(status, 201, detail)
        status, detail = request_json("POST", f"http://127.0.0.1:{a_public}/processes", {
            "process_id": "network-counter", "stream": "network-input", "workflow_type": "HACounter",
            "state_version": 1, "build_id": "chaos-v1", "task_queue": "default",
            "event_time_gate": "immediate", "direct_ingress": True,
            "max_attempts": 10, "batch_max_size": 4, "batch_max_delay": 0,
        })
        self.assertIn(status, (200, 201), detail)

        async def publish(port, first, end):
            handle = ProcessHandle(Client(f"http://127.0.0.1:{port}"), "network-counter",
                                   "network-input", key_field="key", direct_ingress=True)
            for event in range(first, end):
                await handle.send({"key": f"key-{event % 4}", "delta": 1},
                                  event_id=f"event-{event}", event_time=event)

        asyncio.run(asyncio.wait_for(publish(a_public, 0, 1), 30))
        stale = self._poll(a_execution)
        worker = self._start_worker("network-b", b_execution)
        proxy.blocked.set()
        # The publisher remains active through lease expiry and takeover. Only
        # A's storage connections are cut; A itself and its HTTP listeners live.
        asyncio.run(asyncio.wait_for(publish(b_public, 1, 33), 90))
        self._wait_for_owners(b_public, "net-b", minimum_epoch=2)
        self.assertIsNone(node_a.poll(), "isolation unexpectedly killed old owner")
        self.assertIsNone(worker.poll(), "standby worker exited")

        try:
            status, detail = request_json("POST", f"http://127.0.0.1:{a_execution}/internal/v1/process-tasks/complete-batch",
                                          self._completion(stale, 999), timeout=10)
        except (TimeoutError, urllib.error.URLError):
            # A lost response is uncertain; the exact-state check after healing
            # still verifies that this stale transition never became authoritative.
            pass
        else:
            self.assertGreaterEqual(status, 400, f"isolated owner acknowledged a completion: {detail}")

        proxy.blocked.clear()
        self._wait_for_owners(a_public, "net-b", minimum_epoch=2)
        for port in (a_execution, b_execution):
            status, detail = request_json("POST", f"http://127.0.0.1:{port}/internal/v1/process-tasks/complete-batch",
                                          self._completion(stale, 999))
            self.assertGreaterEqual(status, 400, detail)
            status, detail = request_json("POST", f"http://127.0.0.1:{port}/internal/v1/process-tasks/renew", {
                "protocol_version": 1, "lease_token": stale["lease_token"],
                "partition_id": stale["partition_id"], "owner_epoch": stale["owner_epoch"],
                "activation_sequence": stale["activation_sequence"], "extend_seconds": 30,
            })
            self.assertGreaterEqual(status, 400, detail)
        # The returned old core must route new work to the current owner, and a
        # producer retry of the first batch must not increment state twice.
        asyncio.run(asyncio.wait_for(publish(a_public, 0, 65), 90))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status, process = request_json("GET", f"http://127.0.0.1:{b_public}/processes/network-counter")
            self.assertEqual(status, 200, process)
            self.assertEqual(process["failed"], 0, process)
            if process["completed"] == 65:
                break
            self.assertIsNone(worker.poll())
            time.sleep(0.1)
        else:
            self.fail(f"network workload failed to drain: {process}")
        for key in range(4):
            status, state = request_json("GET", f"http://127.0.0.1:{b_public}/processes/network-counter/keys/key-{key}")
            self.assertEqual(status, 200, state)
            self.assertEqual(state["state"], {"total": 17 if key == 0 else 16})
        self._wait_for_owners(a_public, "net-b", minimum_epoch=2)
        self.assertIsNone(node_a.poll())
        self.assertIsNone(node_b.poll())

    def _start_worker(self, name: str, port: int) -> subprocess.Popen[bytes]:
        log = (self.root / f"worker-{name}.log").open("ab")
        worker = subprocess.Popen([
            sys.executable, "-m", "highwater.rust_worker", "ha_workload",
            "--target", f"http://127.0.0.1:{port}", "--process-only",
            "--task-queue", "default", "--process-partitions", "1,2",
        ], cwd=ROOT, env={**os.environ,
            "PYTHONPATH": os.pathsep.join((str(ROOT / "src"), str(ROOT / "tests"))),
            "HIGHWATER_EXECUTION_TOKEN": self.execution_token,
        }, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((worker, log))
        return worker

    def test_live_workers_and_ingestion_survive_both_core_failures(self) -> None:
        from highwater import Client
        from highwater.client import ProcessHandle

        a_public, a_execution, b_public, b_execution = (free_port() for _ in range(4))
        node_a = self._start_node("ha-a", a_public, a_execution)
        self._wait_for_owners(a_public, "ha-a")
        node_b = self._start_node("ha-b", b_public, b_execution)
        self._wait_for_owners(b_public, "ha-a")

        status, detail = request_json("POST", f"http://127.0.0.1:{a_public}/streams", {"name": "ha-input"})
        self.assertEqual(status, 201, detail)
        status, detail = request_json("POST", f"http://127.0.0.1:{a_public}/processes", {
            "process_id": "ha-counter", "stream": "ha-input", "workflow_type": "HACounter",
            "state_version": 1, "build_id": "chaos-v1", "task_queue": "default",
            "event_time_gate": "immediate", "direct_ingress": True,
            "max_attempts": 10, "batch_max_size": 4, "batch_max_delay": 0,
        })
        self.assertIn(status, (200, 201), detail)
        workers = []
        for name, port in (("a", a_execution), ("b", b_execution)):
            workers.append(self._start_worker(name, port))

        def publish_during_failure(port: int, victim: subprocess.Popen[bytes], first: int) -> None:
            admitted = threading.Event()
            errors = []

            async def publish() -> None:
                client = Client(f"http://127.0.0.1:{port}")
                handle = ProcessHandle(client, "ha-counter", "ha-input", key_field="key", direct_ingress=True)
                for event in range(first, first + 48):
                    await handle.send({"key": f"key-{event % 4}", "delta": 1},
                                      event_id=f"event-{event}", event_time=event)
                    if event == first + 7:
                        admitted.set()
                    await asyncio.sleep(0.02)

            def run() -> None:
                try:
                    asyncio.run(asyncio.wait_for(publish(), timeout=90))
                except BaseException as error:
                    errors.append(error)
                    admitted.set()

            publisher = threading.Thread(target=run, daemon=True)
            publisher.start()
            self.assertTrue(admitted.wait(30), "publisher failed to admit initial events")
            if errors:
                raise errors[0]
            victim.kill()
            victim.wait(timeout=10)
            publisher.join(timeout=95)
            self.assertFalse(publisher.is_alive(), "ingestion failed to recover")
            if errors:
                raise errors[0]

        publish_during_failure(b_public, node_a, 0)
        self._wait_for_owners(b_public, "ha-b", minimum_epoch=2)
        replacement = self._start_node("ha-a-replacement", a_public, a_execution)
        self._wait_for_owners(a_public, "ha-b", minimum_epoch=2)
        publish_during_failure(a_public, node_b, 48)
        self._wait_for_owners(a_public, "ha-a-replacement", minimum_epoch=3)

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status, process = request_json("GET", f"http://127.0.0.1:{a_public}/processes/ha-counter")
            self.assertEqual(status, 200, process)
            self.assertEqual(process["failed"], 0, process)
            if process["completed"] == 96:
                break
            self.assertTrue(all(worker.poll() is None for worker in workers), "worker exited during failover")
            time.sleep(0.1)
        else:
            self.fail(f"HA workload failed to drain: {process}")
        for key in range(4):
            status, state = request_json("GET", f"http://127.0.0.1:{a_public}/processes/ha-counter/keys/key-{key}")
            self.assertEqual(status, 200, state)
            self.assertEqual(state["state"], {"total": 24})
        self.assertIsNone(replacement.poll())
        self.assertTrue(all(worker.poll() is None for worker in workers))

    def setUp(self) -> None:
        subprocess.run(
            ["cargo", "build", "--release", "-p", "highwater-server"],
            cwd=ROOT,
            check=True,
        )
        self.root = Path(tempfile.mkdtemp(prefix="highwater-s3-chaos-"))
        self.processes: list[tuple[subprocess.Popen[bytes], Any]] = []
        self.container: str | None = None
        self.aws_env = os.environ.copy()
        self.addCleanup(self._cleanup)
        configured_uri = os.environ.get("HIGHWATER_S3_CHAOS_URI")
        run_id = f"run-{uuid.uuid4().hex}"
        if configured_uri:
            self._load_profile_credentials()
            self.journal_uri = f"{configured_uri.rstrip('/')}/{run_id}"
            self.owner_lease_seconds = 15
        else:
            self.journal_uri = self._start_minio(run_id)
            self.owner_lease_seconds = 2

        self.cluster_token = uuid.uuid4().hex + uuid.uuid4().hex
        self.execution_token = uuid.uuid4().hex + uuid.uuid4().hex
        (self.root / "cluster-token").write_text(self.cluster_token)
        (self.root / "identities.json").write_text(
            json.dumps(
                {
                    "identities": [
                        {
                            "token": self.execution_token,
                            "task_queue": "default",
                            "build_ids": ["chaos-v1"],
                        }
                    ]
                }
            )
        )

    def _load_profile_credentials(self) -> None:
        profile = self.aws_env.get("AWS_PROFILE")
        if not profile or self.aws_env.get("AWS_ACCESS_KEY_ID"):
            return
        exported = subprocess.run(
            [
                "aws",
                "configure",
                "export-credentials",
                "--profile",
                profile,
                "--format",
                "process",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        credentials = json.loads(exported.stdout)
        self.aws_env.update(
            {
                "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
                "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
                "AWS_EC2_METADATA_DISABLED": "true",
            }
        )
        if credentials.get("SessionToken"):
            self.aws_env["AWS_SESSION_TOKEN"] = credentials["SessionToken"]

    def _cleanup(self) -> None:
        for process, log in reversed(self.processes):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            log.close()
        if self.container is not None:
            subprocess.run(
                ["docker", "rm", "-f", self.container],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif not os.environ.get("HIGHWATER_S3_CHAOS_KEEP"):
            subprocess.run(
                ["aws", "s3", "rm", self.journal_uri, "--recursive"],
                env=self.aws_env,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        shutil.rmtree(self.root, ignore_errors=True)

    def _start_minio(self, run_id: str) -> str:
        port = free_port()
        self.container = f"highwater-chaos-{uuid.uuid4().hex[:12]}"
        access_key = "highwater-test-access"
        secret_key = "highwater-test-secret-key"
        image = os.environ.get(
            "HIGHWATER_MINIO_IMAGE",
            "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
        )
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                self.container,
                "--publish",
                f"127.0.0.1:{port}:9000",
                "--env",
                f"MINIO_ROOT_USER={access_key}",
                "--env",
                f"MINIO_ROOT_PASSWORD={secret_key}",
                image,
                "server",
                "/data",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        endpoint = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{endpoint}/minio/health/live", timeout=1):
                    break
            except (OSError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            self.fail("MinIO did not become ready")

        self.aws_env.update(
            {
                "AWS_ACCESS_KEY_ID": access_key,
                "AWS_SECRET_ACCESS_KEY": secret_key,
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_REGION": "us-east-1",
                "AWS_ENDPOINT": endpoint,
                "AWS_ENDPOINT_URL_S3": endpoint,
                "AWS_ALLOW_HTTP": "true",
                "AWS_EC2_METADATA_DISABLED": "true",
            }
        )
        subprocess.run(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3api",
                "create-bucket",
                "--bucket",
                "highwater-chaos",
            ],
            env=self.aws_env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        return f"s3://highwater-chaos/{run_id}"

    def _start_node(self, node_id: str, public_port: int, execution_port: int,
                    *, storage_endpoint: str | None = None) -> subprocess.Popen[bytes]:
        node_root = self.root / node_id
        node_root.mkdir(parents=True, exist_ok=True)
        log = (self.root / f"{node_id}.log").open("ab")
        process = subprocess.Popen(
            [
                str(SERVER),
                "--listen",
                f"127.0.0.1:{public_port}",
                "--execution-listen",
                f"127.0.0.1:{execution_port}",
                "--state-dir",
                str(node_root / "state"),
                "--object-store-dir",
                str(node_root / "objects"),
                "--journal",
                self.journal_uri,
                "--node-id",
                node_id,
                "--advertise-endpoint",
                f"http://127.0.0.1:{execution_port}",
                "--cluster-token-file",
                str(self.root / "cluster-token"),
                "--execution-identity-file",
                str(self.root / "identities.json"),
                "--process-partitions",
                "1,2",
                "--log-shards",
                "3",
                "--lease-seconds",
                str(self.owner_lease_seconds),
            ],
            cwd=ROOT,
            env={**self.aws_env, **({"AWS_ENDPOINT": storage_endpoint,
                                    "AWS_ENDPOINT_URL_S3": storage_endpoint} if storage_endpoint else {})},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append((process, log))
        self._wait_for_node(process, public_port, log)
        return process

    def _wait_for_node(self, process: subprocess.Popen[bytes], port: int, log: Any) -> None:
        deadline = time.monotonic() + 60
        url = f"http://127.0.0.1:{port}/admin/process-partitions"
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log.flush()
                self.fail((self.root / Path(log.name).name).read_text())
            try:
                status, _ = request_json("GET", url, timeout=2)
            except (TimeoutError, urllib.error.URLError):
                time.sleep(0.1)
                continue
            if status == 200:
                return
            time.sleep(0.1)
        self.fail(f"node on port {port} did not become ready")

    def _wait_for_owners(self, port: int, node_id: str, *, minimum_epoch: int = 1) -> list[dict[str, Any]]:
        deadline = time.monotonic() + 60
        url = f"http://127.0.0.1:{port}/admin/process-partitions"
        while time.monotonic() < deadline:
            status, owners = request_json("GET", url, timeout=2)
            if status == 200 and len(owners) == 2 and all(
                owner["node_id"] == node_id
                and owner["status"] == "ACTIVE"
                and owner["epoch"] >= minimum_epoch
                for owner in owners
            ):
                return owners
            time.sleep(0.2)
        self.fail(f"partitions did not become active on {node_id}")

    def _poll(self, execution_port: int) -> dict[str, Any]:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for partition in (1, 2):
                status, activation = request_json(
                    "POST",
                    f"http://127.0.0.1:{execution_port}/internal/v1/process-tasks/poll-batch",
                    {
                        "protocol_version": 1,
                        "worker_id": "chaos-worker",
                        "execution_token": self.execution_token,
                        "task_queue": "default",
                        "build_ids": ["chaos-v1"],
                        "lease_seconds": 1,
                        "partition_id": partition,
                    },
                    timeout=10,
                )
                if status == 200 and activation is not None:
                    return activation
            time.sleep(0.2)
        self.fail("no process activation became available")

    @staticmethod
    def _completion(
        activation: dict[str, Any], balance: int, emit: Any = None,
    ) -> dict[str, Any]:
        return {
            "protocol_version": 1,
            "lease_token": activation["lease_token"],
            "partition_id": activation["partition_id"],
            "owner_epoch": activation["owner_epoch"],
            "activation_sequence": activation["activation_sequence"],
            "items": [
                {
                    "result": {
                        "__highwater_transition__": True,
                        "state": {"balance": balance},
                        "emit": emit,
                    }
                }
                for _ in activation["envelopes"]
            ],
        }

    def test_owner_crash_fences_stale_work_and_restores_from_checkpoint(self) -> None:
        a_public, a_execution, b_public, b_execution = (free_port() for _ in range(4))
        node_a = self._start_node("node-a", a_public, a_execution)
        owners_a = self._wait_for_owners(a_public, "node-a")
        node_b = self._start_node("node-b", b_public, b_execution)
        self._wait_for_owners(b_public, "node-a")

        status, _ = request_json(
            "POST", f"http://127.0.0.1:{a_public}/streams", {"name": "chaos-input"}
        )
        self.assertEqual(status, 201)
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{a_public}/processes",
            {
                "process_id": "chaos-process",
                "stream": "chaos-input",
                "workflow_type": "ChaosProcess",
                "state_version": 1,
                "build_id": "chaos-v1",
                "task_queue": "default",
                "event_time_gate": "immediate",
                "direct_ingress": True,
                "batch_max_size": 1,
                "batch_max_delay": 0,
            },
        )
        self.assertIn(status, (200, 201))
        first_event = {
            "records": [
                {
                    "partition": 0,
                    "event_time": 1,
                    "key": "account-a",
                    "value": {"delta": 5},
                    "kind": "upsert",
                    "event_id": "event-1",
                }
            ]
        }
        status, admitted = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/events",
            first_event,
        )
        self.assertEqual(status, 201)
        self.assertEqual(admitted[0]["disposition"], "accepted")
        stale_activation = self._poll(a_execution)

        node_a.kill()
        node_a.wait(timeout=10)
        prior_epochs = {owner["partition_id"]: owner["epoch"] for owner in owners_a}
        owners_b = self._wait_for_owners(b_public, "node-b", minimum_epoch=2)
        self.assertTrue(
            all(owner["epoch"] > prior_epochs[owner["partition_id"]] for owner in owners_b)
        )

        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{b_execution}/internal/v1/process-tasks/complete-batch",
            self._completion(stale_activation, 999),
        )
        self.assertGreaterEqual(status, 400)

        recovered_activation = self._poll(b_execution)
        self.assertGreater(recovered_activation["owner_epoch"], stale_activation["owner_epoch"])
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{b_execution}/internal/v1/process-tasks/complete-batch",
            self._completion(recovered_activation, 5),
        )
        self.assertEqual(status, 200)
        status, state = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/keys/account-a",
        )
        self.assertEqual(status, 200)
        self.assertEqual(state["state"], {"balance": 5})

        status, _ = request_json(
            "POST", f"http://127.0.0.1:{b_public}/admin/checkpoints", {}
        )
        self.assertEqual(status, 201)
        node_b.kill()
        node_b.wait(timeout=10)
        shutil.rmtree(self.root / "node-b")

        node_b = self._start_node("node-b", b_public, b_execution)
        self._wait_for_owners(b_public, "node-b", minimum_epoch=3)
        status, state = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/keys/account-a",
        )
        self.assertEqual(status, 200)
        self.assertEqual(state["state"], {"balance": 5})

        status, duplicate = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/events",
            first_event,
        )
        self.assertEqual(status, 201, duplicate)
        self.assertEqual(duplicate[0]["disposition"], "duplicate")

        second_event = {
            "records": [
                {
                    "partition": 0,
                    "event_time": 2,
                    "key": "account-a",
                    "value": {"delta": 7},
                    "kind": "upsert",
                    "event_id": "event-2",
                }
            ]
        }
        if self.container is not None:
            subprocess.run(
                ["docker", "pause", self.container],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            try:
                with self.assertRaises((TimeoutError, urllib.error.URLError)):
                    request_json(
                        "POST",
                        f"http://127.0.0.1:{b_public}/processes/chaos-process/events",
                        second_event,
                        timeout=0.5,
                    )
            finally:
                subprocess.run(
                    ["docker", "unpause", self.container],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )

        status, retried = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/events",
            second_event,
        )
        self.assertEqual(status, 201)
        self.assertIn(retried[0]["disposition"], ("accepted", "duplicate"))
        second_activation = self._poll(b_execution)
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{b_execution}/internal/v1/process-tasks/complete-batch",
            self._completion(second_activation, 12),
        )
        self.assertEqual(status, 200)
        status, state = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/chaos-process/keys/account-a",
        )
        self.assertEqual(status, 200)
        self.assertEqual(state["state"], {"balance": 12})
        status, process = request_json(
            "GET", f"http://127.0.0.1:{b_public}/processes/chaos-process"
        )
        self.assertEqual(status, 200)
        self.assertEqual(process["completed"], 2)
        self.assertIsNone(node_b.poll())

    def test_non_owner_publishes_journal_vector_checkpoint_and_restores_it(self) -> None:
        a_public, a_execution, b_public, b_execution = (free_port() for _ in range(4))
        node_a = self._start_node("node-a", a_public, a_execution)
        self._wait_for_owners(a_public, "node-a")
        node_b = self._start_node("node-b", b_public, b_execution)
        self._wait_for_owners(b_public, "node-a")

        status, _ = request_json(
            "POST", f"http://127.0.0.1:{a_public}/streams", {"name": "checkpoint-input"}
        )
        self.assertEqual(status, 201)
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{a_public}/processes",
            {
                "process_id": "checkpoint-process",
                "stream": "checkpoint-input",
                "workflow_type": "CheckpointProcess",
                "state_version": 1,
                "build_id": "chaos-v1",
                "task_queue": "default",
                "event_time_gate": "immediate",
                "direct_ingress": True,
                "batch_max_size": 1,
                "batch_max_delay": 0,
            },
        )
        self.assertIn(status, (200, 201))
        status, admitted = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/processes/checkpoint-process/events",
            {
                "records": [
                    {
                        "partition": 0,
                        "event_time": 1,
                        "key": "account-a",
                        "value": {"delta": 5},
                        "kind": "upsert",
                        "event_id": "checkpoint-event-1",
                    }
                ]
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(admitted[0]["disposition"], "accepted")

        activation = self._poll(a_execution)
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{a_execution}/internal/v1/process-tasks/complete-batch",
            self._completion(activation, 5, {"balance": 5}),
        )
        self.assertEqual(status, 200)

        status, outcome = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/checkpoint-process/"
            "keys/account-a/events/checkpoint-event-1",
        )
        self.assertEqual(status, 200, outcome)
        self.assertEqual(outcome["status"], "COMMITTED")
        self.assertEqual(len(outcome["output_message_ids"]), 1)
        sink = urllib.parse.quote("process:checkpoint-process", safe="")
        status, delivered = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/sinks/{sink}/poll",
            {"consumer_id": "checkpoint-consumer", "lease_seconds": 30},
        )
        self.assertEqual(status, 200, delivered)
        message_id = urllib.parse.quote(delivered["message_id"], safe="")
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/sinks/{sink}/messages/{message_id}/ack",
            {"consumer_id": "checkpoint-consumer"},
        )
        self.assertEqual(status, 200)

        # Node B owns no process partition. Its checkpoint must synchronize the
        # authoritative journal vector, include node A's committed transition,
        # and publish without an unverifiable owner-local acknowledgement.
        status, checkpoint = request_json(
            "POST", f"http://127.0.0.1:{b_public}/admin/checkpoints", {}
        )
        self.assertEqual(status, 201, checkpoint)
        self.assertTrue(checkpoint["shard_sequences"])

        status, barrier = request_json(
            "POST", f"http://127.0.0.1:{b_public}/admin/checkpoint-barriers", {}
        )
        self.assertEqual(status, 201, barrier)
        self.assertEqual(barrier["status"], "COMPLETE")
        self.assertEqual(barrier["expected_nodes"], [])
        self.assertEqual(barrier["acknowledgements"], {})
        status, _ = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/admin/checkpoint-barriers/"
            f"{barrier['checkpoint_id']}/acks/node-a",
            {"state_handle": "unverified", "key_group_epochs": {}},
        )
        self.assertGreaterEqual(status, 400)

        node_a.kill()
        node_a.wait(timeout=10)
        node_b.kill()
        node_b.wait(timeout=10)
        shutil.rmtree(self.root / "node-b")

        node_b = self._start_node("node-b", b_public, b_execution)
        self._wait_for_owners(b_public, "node-b", minimum_epoch=2)
        status, state = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/checkpoint-process/keys/account-a",
        )
        self.assertEqual(status, 200)
        self.assertEqual(state["state"], {"balance": 5})
        status, outcome = request_json(
            "GET",
            f"http://127.0.0.1:{b_public}/processes/checkpoint-process/"
            "keys/account-a/events/checkpoint-event-1",
        )
        self.assertEqual(status, 200, outcome)
        self.assertEqual(outcome["status"], "COMMITTED")
        status, delivered = request_json(
            "POST",
            f"http://127.0.0.1:{b_public}/sinks/{sink}/poll",
            {"consumer_id": "checkpoint-consumer", "lease_seconds": 30},
        )
        self.assertEqual(status, 204, delivered)
        self.assertIsNone(delivered)
        self.assertIsNone(node_b.poll())


if __name__ == "__main__":
    unittest.main()
