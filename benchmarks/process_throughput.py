from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from dataclasses import dataclass

from temporal_code import Client, ProcessOptions, streaming


@dataclass(frozen=True)
class CounterEvent:
    key: str
    amount: int


@streaming.process(key="key", build_id="process-throughput-v1")
@dataclass
class CounterProcess:
    total: int = 0

    @streaming.event
    async def apply(self, event: CounterEvent):
        self.total += event.amount


@streaming.process(key="key", build_id="process-throughput-batch-v1")
class BatchedCounterProcess:
    @streaming.batch(max_size=1_024, max_delay=0.002)
    async def apply(self, events: list[CounterEvent], contexts):
        return [
            streaming.transition(
                state={"total": (context.state or {}).get("total", 0) + event.amount},
            )
            for event, context in zip(events, contexts, strict=True)
        ]


async def benchmark(
    target: str,
    events: int,
    publishers: int,
    batch_size: int,
    activation_batch_size: int,
    activation_batch_delay: float,
    max_concurrency: int,
    handler: str,
) -> None:
    client = Client(target, poll_interval=0.001)
    process_id = f"throughput-{uuid.uuid4().hex[:8]}"
    definition = BatchedCounterProcess if handler == "batch" else CounterProcess
    handle = await client.start(
        definition,
        process_id=process_id,
        options=ProcessOptions(
            key="key",
            max_concurrency=max_concurrency,
            capacity=events * 2 + 1,
            batch_size=activation_batch_size,
            batch_delay=activation_batch_delay,
        ),
    )
    semaphore = asyncio.Semaphore(publishers)

    async def publish(batch: list[CounterEvent]) -> None:
        async with semaphore:
            await handle.send_many(batch)

    values = [CounterEvent(f"key-{index}", 1) for index in range(events)]
    started = time.perf_counter()
    await asyncio.gather(*(
        publish(values[index:index + batch_size])
        for index in range(0, events, batch_size)
    ))
    admitted = time.perf_counter()
    if handle.direct_ingress:
        await handle.drain(timeout=300)
    else:
        await handle.finish(timeout=300)
    finished = time.perf_counter()
    print(json.dumps({
        "events": events,
        "publishers": publishers,
        "batch_size": batch_size,
        "activation_batch_size": activation_batch_size,
        "activation_batch_delay": activation_batch_delay,
        "max_concurrency": max_concurrency,
        "handler": handler,
        "admission_events_per_second": events / (admitted - started),
        "completed_events_per_second": events / (finished - started),
        "admission_seconds": admitted - started,
        "end_to_end_seconds": finished - started,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--events", type=int, default=2_000)
    parser.add_argument("--publishers", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--activation-batch-size", type=int, default=1_024)
    parser.add_argument("--activation-batch-delay", type=float, default=0.002)
    parser.add_argument("--max-concurrency", type=int, default=8_192)
    parser.add_argument("--handler", choices=("batch", "event"), default="batch")
    args = parser.parse_args()
    asyncio.run(benchmark(
        args.target,
        args.events,
        args.publishers,
        args.batch_size,
        args.activation_batch_size,
        args.activation_batch_delay,
        args.max_concurrency,
        args.handler,
    ))
