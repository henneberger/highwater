from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict

from .autoscaling import AutoscalingPolicy, WorkloadSample, recommend_replicas
from .client import Client
from .kubernetes import KubernetesScaleClient


def _sample(process: dict, observed_at: float) -> WorkloadSample:
    return WorkloadSample(
        observed_at=observed_at,
        pending=int(process["pending"]),
        running=int(process["running"]),
        completed=int(process["completed"]),
    )


async def run(arguments: argparse.Namespace) -> None:
    client = Client(arguments.target, api_key=arguments.api_key)
    kubernetes = (
        KubernetesScaleClient.from_environment(arguments.kubernetes_namespace)
        if arguments.kubernetes_deployment
        else None
    )
    current_replicas = (
        await asyncio.to_thread(kubernetes.get, arguments.kubernetes_deployment)
    ).replicas if kubernetes else arguments.current_replicas
    previous = _sample(await client.process(arguments.process), time.time())
    seconds_below_target = arguments.seconds_below_target
    while True:
        await asyncio.sleep(arguments.sample_interval)
        current = _sample(await client.process(arguments.process), time.time())
        decision = recommend_replicas(
            previous,
            current,
            current_replicas=current_replicas,
            partitions=arguments.partitions,
            policy=AutoscalingPolicy(
                min_replicas=arguments.min_replicas,
                max_replicas=arguments.max_replicas,
                target_events_per_second_per_replica=arguments.target_events_per_second,
                target_backlog_per_replica=arguments.target_backlog,
                headroom=arguments.headroom,
                scale_down_after=arguments.scale_down_after,
            ),
            seconds_below_target=seconds_below_target,
        )
        payload = asdict(decision)
        payload["partition_assignments"] = [
            list(assignment) for assignment in decision.partition_assignments
        ]
        print(json.dumps(payload, sort_keys=True), flush=True)
        if kubernetes and decision.desired_replicas != current_replicas:
            scale = await asyncio.to_thread(
                kubernetes.get, arguments.kubernetes_deployment
            )
            updated = await asyncio.to_thread(
                kubernetes.set,
                arguments.kubernetes_deployment,
                decision.desired_replicas,
                scale.resource_version,
            )
            current_replicas = updated.replicas
        else:
            current_replicas = decision.desired_replicas
        seconds_below_target = (
            seconds_below_target + arguments.sample_interval
            if current.pending == 0 and decision.reason != "traffic"
            else 0
        )
        previous = current
        if not arguments.watch:
            return


def main() -> None:
    parser = argparse.ArgumentParser(prog="highwater-autoscaler")
    parser.add_argument("--process", required=True)
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--api-key")
    parser.add_argument("--current-replicas", type=int)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--min-replicas", type=int, default=1)
    parser.add_argument("--max-replicas", type=int, default=64)
    parser.add_argument("--target-events-per-second", type=float, default=5_000)
    parser.add_argument("--target-backlog", type=int, default=5_000)
    parser.add_argument("--headroom", type=float, default=1.25)
    parser.add_argument("--scale-down-after", type=float, default=300)
    parser.add_argument("--seconds-below-target", type=float, default=0)
    parser.add_argument("--sample-interval", type=float, default=5)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--kubernetes-deployment")
    parser.add_argument("--kubernetes-namespace")
    arguments = parser.parse_args()
    if arguments.sample_interval <= 0:
        parser.error("--sample-interval must be positive")
    if arguments.current_replicas is None and not arguments.kubernetes_deployment:
        parser.error("--current-replicas is required without --kubernetes-deployment")
    if arguments.kubernetes_deployment and not arguments.watch:
        parser.error("--kubernetes-deployment requires --watch")
    asyncio.run(run(arguments))


if __name__ == "__main__":
    main()
