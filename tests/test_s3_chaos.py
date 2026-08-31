from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "release" / "temporal-code-server"
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
        payload = error.read()
        try:
            detail = json.loads(payload) if payload else None
        except json.JSONDecodeError:
            detail = payload.decode(errors="replace")
        return error.code, detail


@unittest.skipUnless(
    RUN_CHAOS,
    "set HIGHWATER_S3_CHAOS=1 to run the multi-process S3 failure test",
)
class S3ChaosTest(unittest.TestCase):
    def setUp(self) -> None:
        subprocess.run(
            ["cargo", "build", "--release", "-p", "temporal-code-server"],
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
            self.journal_uri = f"{configured_uri.rstrip('/')}/{run_id}"
        else:
            self.journal_uri = self._start_minio(run_id)

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
        deadline = time.monotonic() + 30
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

    def _start_node(self, node_id: str, public_port: int, execution_port: int) -> subprocess.Popen[bytes]:
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
                "2",
            ],
            cwd=ROOT,
            env=self.aws_env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append((process, log))
        self._wait_for_node(process, public_port, log)
        return process

    def _wait_for_node(self, process: subprocess.Popen[bytes], port: int, log: Any) -> None:
        deadline = time.monotonic() + 30
        url = f"http://127.0.0.1:{port}/admin/process-partitions"
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log.flush()
                self.fail((self.root / Path(log.name).name).read_text())
            try:
                status, _ = request_json("GET", url, timeout=1)
            except urllib.error.URLError:
                time.sleep(0.1)
                continue
            if status == 200:
                return
            time.sleep(0.1)
        self.fail(f"node on port {port} did not become ready")

    def _wait_for_owners(self, port: int, node_id: str, *, minimum_epoch: int = 1) -> list[dict[str, Any]]:
        deadline = time.monotonic() + 15
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
        deadline = time.monotonic() + 15
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
                    timeout=2,
                )
                if status == 200 and activation is not None:
                    return activation
            time.sleep(0.2)
        self.fail("no process activation became available")

    @staticmethod
    def _completion(activation: dict[str, Any], balance: int) -> dict[str, Any]:
        return {
            "protocol_version": 1,
            "lease_token": activation["lease_token"],
            "partition_id": activation["partition_id"],
            "owner_epoch": activation["owner_epoch"],
            "activation_sequence": activation["activation_sequence"],
            "items": [
                {
                    "result": {
                        "__temporal_code_transition__": True,
                        "state": {"balance": balance},
                        "emit": None,
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
        self.assertEqual(status, 201)
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


if __name__ == "__main__":
    unittest.main()
