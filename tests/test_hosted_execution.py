from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "highwater-server"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    token: str | None = None,
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    value = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(value, timeout=10) as response:
            data = response.read()
            return response.status, json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        data = error.read()
        return error.code, json.loads(data) if data else None


class HostedExecutionTest(unittest.TestCase):
    def test_public_and_execution_boundaries_enforce_scoped_identities(self) -> None:
        if not SERVER.is_file():
            self.skipTest("build highwater-server to run hosted execution test")
        public_port = free_port()
        execution_port = free_port()
        api_token = "public-" + "a" * 40
        execution_token = "worker-" + "b" * 40
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "api-token").write_text(api_token)
            (root / "identities.json").write_text(json.dumps({
                "identities": [{
                    "token": execution_token,
                    "task_queue": "hosted-app",
                    "build_ids": ["hosted-build-v1"],
                }],
            }))
            server = subprocess.Popen(
                [
                    str(SERVER),
                    "--listen", f"127.0.0.1:{public_port}",
                    "--execution-listen", f"127.0.0.1:{execution_port}",
                    "--state-dir", str(root / "state"),
                    "--object-store-dir", str(root / "objects"),
                    "--api-token-file", str(root / "api-token"),
                    "--execution-identity-file", str(root / "identities.json"),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        with socket.create_connection(("127.0.0.1", public_port), timeout=0.1):
                            with socket.create_connection(("127.0.0.1", execution_port), timeout=0.1):
                                break
                    except OSError:
                        if server.poll() is not None:
                            self.fail("hosted server stopped during startup")
                        time.sleep(0.05)
                else:
                    self.fail("hosted server did not start")

                public = f"http://127.0.0.1:{public_port}"
                execution = f"http://127.0.0.1:{execution_port}"
                self.assertEqual(request("GET", f"{public}/health")[0], 200)
                self.assertEqual(request("POST", f"{public}/streams", {"name": "events"})[0], 401)
                self.assertEqual(
                    request("POST", f"{public}/streams", {"name": "events"}, token=api_token)[0],
                    201,
                )
                self.assertIn(
                    request(
                        "POST",
                        f"{public}/internal/v1/process-tasks/poll-batch",
                        {},
                        token=api_token,
                    )[0],
                    (401, 404),
                )

                poll = {
                    "protocol_version": 1,
                    "worker_id": "hosted-worker",
                    "execution_token": execution_token,
                    "task_queue": "hosted-app",
                    "build_ids": ["hosted-build-v1"],
                    "lease_seconds": 5,
                    "partition_id": 1,
                }
                status, activation = request(
                    "POST",
                    f"{execution}/internal/v1/process-tasks/poll-batch",
                    poll,
                )
                self.assertEqual(status, 200)
                self.assertIsNone(activation)

                unauthorized = {**poll, "execution_token": "wrong-" + "c" * 40}
                self.assertNotEqual(
                    request(
                        "POST",
                        f"{execution}/internal/v1/process-tasks/poll-batch",
                        unauthorized,
                    )[0],
                    200,
                )
                wrong_build = {**poll, "build_ids": ["other-build"]}
                self.assertNotEqual(
                    request(
                        "POST",
                        f"{execution}/internal/v1/process-tasks/poll-batch",
                        wrong_build,
                    )[0],
                    200,
                )
                self.assertEqual(request("GET", f"{execution}/streams/events")[0], 404)
            finally:
                server.terminate()
                server.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
