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
    environment = os.environ.copy()
    environment["PYTHONPATH"] = f"{ROOT / 'src'}:{ROOT}"
    processes: list[subprocess.Popen[bytes]] = []
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
                f"127.0.0.1:{port}",
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
            for index, assignment in enumerate(assignments):
                worker = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "highwater.rust_worker",
                        "benchmarks.process_throughput",
                        "--target",
                        target,
                        "--process-partitions",
                        ",".join(str(partition) for partition in assignment),
                        "--process-only",
                    ],
                    cwd=ROOT,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                processes.append(worker)
            benchmark_process = subprocess.run(
                [
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
                ],
                cwd=ROOT,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=arguments.timeout,
            )
            result = json.loads(benchmark_process.stdout)
            result.update(
                {
                    "execution_instances": len(assignments),
                    "partitions": arguments.partitions,
                    "partition_assignments": assignments,
                    "durability_boundary": "authoritative append before admission and completion acknowledgement",
                }
            )
            return result
        finally:
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
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--minimum-throughput", type=float, default=0)
    arguments = parser.parse_args()
    if arguments.events <= 0:
        parser.error("--events must be positive")
    if arguments.execution_instances <= 0:
        parser.error("--execution-instances must be positive")
    if not 1 <= arguments.partitions <= 255:
        parser.error("--partitions must be between 1 and 255")
    if arguments.runs <= 0:
        parser.error("--runs must be positive")
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
