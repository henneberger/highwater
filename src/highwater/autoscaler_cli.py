from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict

from .autoscaling import AutoscalingPolicy, WorkloadSample, recommend_replicas
from .client import Client


def _sample(process: dict, observed_at: float) -> WorkloadSample:
    return WorkloadSample(
        observed_at=observed_at,
        pending=int(process["pending"]),
        running=int(process["running"]),
        completed=int(process["completed"]),
    )


async def run(arguments: argparse.Namespace) -> None:
    client = Client(arguments.target, api_key=arguments.api_key)
    previous = _sample(await client.process(arguments.process), time.time())
    await asyncio.sleep(arguments.sample_interval)
    current = _sample(await client.process(arguments.process), time.time())
    decision = recommend_replicas(
        previous,
        current,
        current_replicas=arguments.current_replicas,
        partitions=arguments.partitions,
        policy=AutoscalingPolicy(
            min_replicas=arguments.min_replicas,
            max_replicas=arguments.max_replicas,
            target_events_per_second_per_replica=arguments.target_events_per_second,
            target_backlog_per_replica=arguments.target_backlog,
            headroom=arguments.headroom,
            scale_down_after=arguments.scale_down_after,
        ),
        seconds_below_target=arguments.seconds_below_target,
    )
    payload = asdict(decision)
    payload["partition_assignments"] = [
        list(assignment) for assignment in decision.partition_assignments
    ]
    print(json.dumps(payload, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(prog="highwater-autoscaler")
    parser.add_argument("--process", required=True)
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--api-key")
    parser.add_argument("--current-replicas", type=int, required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--min-replicas", type=int, default=1)
    parser.add_argument("--max-replicas", type=int, default=64)
    parser.add_argument("--target-events-per-second", type=float, default=5_000)
    parser.add_argument("--target-backlog", type=int, default=5_000)
    parser.add_argument("--headroom", type=float, default=1.25)
    parser.add_argument("--scale-down-after", type=float, default=300)
    parser.add_argument("--seconds-below-target", type=float, default=0)
    parser.add_argument("--sample-interval", type=float, default=5)
    arguments = parser.parse_args()
    if arguments.sample_interval <= 0:
        parser.error("--sample-interval must be positive")
    asyncio.run(run(arguments))


if __name__ == "__main__":
    main()
