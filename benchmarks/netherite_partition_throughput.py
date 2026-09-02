from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for_server(port: int, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server stopped with exit code {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("server did not begin listening")


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def assigned_partitions(partitions: int, instances: int) -> list[list[int]]:
    assignments = [[] for _ in range(instances)]
    for index, partition in enumerate(range(1, partitions + 1)):
        assignments[index % instances].append(partition)
    return [assignment for assignment in assignments if assignment]


def benchmark(arguments: argparse.Namespace) -> dict[str, Any]:
    server_binary = (ROOT / arguments.server).resolve()
    if not server_binary.is_file():
        raise FileNotFoundError(
            f"{server_binary} does not exist; run cargo build --release -p highwater-server"
        )
    port = available_port()
    target = f"http://127.0.0.1:{port}"
    worker_target = target
    environment = os.environ.copy()
    environment["PYTHONPATH"] = f"{ROOT / 'src'}:{ROOT}"
    processes: list[subprocess.Popen[bytes]] = []
    containers: list[str] = []
    with tempfile.TemporaryDirectory(prefix="highwater-netherite-benchmark-") as temporary:
        root = Path(temporary)
        server = subprocess.Popen(
            [
                str(server_binary),
                "--state-dir",
                str(root / "state"),
                "--object-store-dir",
                str(root / "objects"),
                "--listen",
                f"{'0.0.0.0' if arguments.worker_runtime == 'docker' else '127.0.0.1'}:{port}",
                "--node-id",
                "benchmark",
                "--key-groups",
                str(max(128, arguments.partitions * 8)),
                "--lease-seconds",
                "30",
                "--log-shards",
                str(arguments.partitions + 1),
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(server)
        try:
            wait_for_server(port, server)
            assignments = assigned_partitions(
                arguments.partitions, arguments.execution_instances
            )
            if arguments.worker_runtime == "docker":
                worker_target = f"http://host.docker.internal:{port}"
            for index, assignment in enumerate(assignments):
                worker_command = [
                    sys.executable,
                    "-m",
                    "highwater.rust_worker",
                    "benchmarks.process_throughput",
                    "--target",
                    worker_target,
                    "--process-partitions",
                    ",".join(str(partition) for partition in assignment),
                    "--process-only",
                ]
                if arguments.worker_runtime == "docker":
                    name = f"highwater-benchmark-{os.getpid()}-{index}"
                    containers.append(name)
                    worker_command = [
                        "docker", "run", "--rm", "--name", name,
                        "--add-host", "host.docker.internal:host-gateway",
                        "--read-only",
                        "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m",
                        "--cap-drop", "ALL",
                        "--security-opt", "no-new-privileges",
                        "--pids-limit", str(arguments.sandbox_pids),
                        "--memory", arguments.sandbox_memory,
                        "--cpus", str(arguments.sandbox_cpus),
                        "--volume", f"{ROOT / 'benchmarks'}:/workload:ro",
                        "--env", "PYTHONPATH=/workload",
                        arguments.worker_image,
                        "process_throughput",
                        "--target", worker_target,
                        "--process-partitions",
                        ",".join(str(partition) for partition in assignment),
                        "--process-only",
                    ]
                worker = subprocess.Popen(
                    worker_command,
                    cwd=ROOT,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                processes.append(worker)
            if arguments.worker_startup_delay > 0:
                time.sleep(arguments.worker_startup_delay)
                stopped = [process.returncode for process in processes[1:] if process.poll() is not None]
                if stopped:
                    raise RuntimeError(f"execution worker stopped during startup: {stopped}")
            workload_command = [
                    sys.executable,
                    str(ROOT / "benchmarks/process_throughput.py"),
                    "--target",
                    target,
                    "--events",
                    str(arguments.events),
                    "--publishers",
                    str(arguments.publishers),
                    "--batch-size",
                    str(arguments.batch_size),
                    "--activation-batch-size",
                    str(arguments.activation_batch_size),
                    "--activation-batch-delay",
                    str(arguments.activation_batch_delay),
                    "--max-concurrency",
                    str(arguments.max_concurrency),
                    "--handler",
                    arguments.handler,
                    "--workload",
                    arguments.workload,
                    "--active-keys",
                    str(arguments.active_keys),
                ]
            try:
                benchmark_process = subprocess.run(
                    workload_command,
                    cwd=ROOT,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=arguments.timeout,
                )
            except subprocess.TimeoutExpired as error:
                diagnostics = error.stderr or ""
                if isinstance(diagnostics, bytes):
                    diagnostics = diagnostics.decode(errors="replace")
                raise TimeoutError(
                    f"workload benchmark exceeded {arguments.timeout:g}s"
                    + (f":\n{diagnostics.strip()}" if diagnostics.strip() else "")
                ) from error
            if benchmark_process.returncode != 0:
                raise RuntimeError(
                    "workload benchmark failed:\n" + benchmark_process.stderr.strip()
                )
            result = json.loads(benchmark_process.stdout)
            result.update(
                {
                    "execution_instances": len(assignments),
                    "partitions": arguments.partitions,
                    "partition_assignments": assignments,
                    "durability_boundary": "authoritative append before admission and completion acknowledgement",
                    "worker_runtime": arguments.worker_runtime,
                }
            )
            return result
        finally:
            if containers:
                subprocess.run(
                    ["docker", "rm", "--force", *containers],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            for process in reversed(processes):
                stop(process)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server", default="target/release/highwater-server"
    )
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--execution-instances", type=int, default=10)
    parser.add_argument("--partitions", type=int, default=10)
    parser.add_argument("--publishers", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--activation-batch-size", type=int, default=12_000)
    parser.add_argument("--activation-batch-delay", type=float, default=0.002)
    parser.add_argument("--max-concurrency", type=int, default=100_000)
    parser.add_argument("--handler", choices=("batch", "event"), default="batch")
    parser.add_argument("--workload", choices=("counter", "shopping"), default="shopping")
    parser.add_argument("--active-keys", type=int, default=20_000)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--minimum-throughput", type=float, default=0)
    parser.add_argument("--worker-runtime", choices=("host", "docker"), default="host")
    parser.add_argument("--worker-image", default="highwater-worker:release-test")
    parser.add_argument("--sandbox-cpus", type=float, default=2)
    parser.add_argument("--sandbox-memory", default="512m")
    parser.add_argument("--sandbox-pids", type=int, default=128)
    parser.add_argument("--worker-startup-delay", type=float, default=0)
    arguments = parser.parse_args()
    if arguments.events <= 0:
        parser.error("--events must be positive")
    if arguments.execution_instances <= 0:
        parser.error("--execution-instances must be positive")
    if not 1 <= arguments.partitions <= 255:
        parser.error("--partitions must be between 1 and 255")
    if arguments.runs <= 0:
        parser.error("--runs must be positive")
    if arguments.active_keys < 0:
        parser.error("--active-keys must be non-negative")
    if arguments.sandbox_cpus <= 0 or arguments.sandbox_pids <= 0:
        parser.error("sandbox CPU and process limits must be positive")
    if arguments.worker_startup_delay < 0:
        parser.error("--worker-startup-delay must be non-negative")
    results = [benchmark(arguments) for _ in range(arguments.runs)]
    completed = [result["completed_events_per_second"] for result in results]
    admitted = [result["admission_events_per_second"] for result in results]
    report = {
        "runs": results,
        "summary": {
            "run_count": arguments.runs,
            "events_per_run": arguments.events,
            "execution_instances": results[0]["execution_instances"],
            "partitions": arguments.partitions,
            "handler": arguments.handler,
            "workload": arguments.workload,
            "active_keys": results[0]["active_keys"],
            "median_admission_events_per_second": statistics.median(admitted),
            "median_completed_events_per_second": statistics.median(completed),
            "minimum_completed_events_per_second": min(completed),
            "maximum_completed_events_per_second": max(completed),
            "durability_boundary": results[0]["durability_boundary"],
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["summary"]["minimum_completed_events_per_second"] < arguments.minimum_throughput:
        raise SystemExit(
            "completed throughput "
            f"{report['summary']['minimum_completed_events_per_second']:.0f} events/s "
            f"is below required {arguments.minimum_throughput:.0f} events/s"
        )


if __name__ == "__main__":
    main()
