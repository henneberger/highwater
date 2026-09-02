from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from dataclasses import dataclass

from highwater import Client, ProcessOptions, streaming


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


@dataclass(frozen=True)
class ProductView:
    session_id: str
    product_id: str
    category: str
    affinity: float


@streaming.process(key="session_id", build_id="shopping-throughput-v1")
class ShoppingSessionProcess:
    @streaming.batch(max_size=1_024, max_delay=0.002)
    async def recommend(self, events: list[ProductView], contexts):
        transitions = []
        for event, context in zip(events, contexts, strict=True):
            previous = context.state or {}
            recent = [*previous.get("recent", []), event.category][-5:]
            views = previous.get("views", 0) + 1
            category_matches = sum(value == event.category for value in recent)
            score = event.affinity * (1 + category_matches / len(recent))
            recommendation = (
                {
                    "session_id": event.session_id,
                    "product_id": event.product_id,
                    "score": score,
                    "context_size": len(recent),
                }
                if views % 5 == 0
                else None
            )
            transitions.append(streaming.transition(
                state={
                    "recent": recent,
                    "views": views,
                    "last_product_id": event.product_id,
                },
                emit=recommendation,
            ))
        return transitions


async def benchmark(
    target: str,
    events: int,
    publishers: int,
    batch_size: int,
    activation_batch_size: int,
    activation_batch_delay: float,
    max_concurrency: int,
    handler: str,
    workload: str,
    active_keys: int,
) -> None:
    client = Client(target, poll_interval=0.001)
    process_id = f"throughput-{uuid.uuid4().hex[:8]}"
    if workload == "shopping":
        definition = ShoppingSessionProcess
    else:
        definition = BatchedCounterProcess if handler == "batch" else CounterProcess
    handle = await client.start(
        definition,
        process_id=process_id,
        options=ProcessOptions(
            key="session_id" if workload == "shopping" else "key",
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

    if workload == "shopping":
        categories = ("books", "games", "home", "outdoors", "audio")
        session_count = active_keys or events
        values = [
            ProductView(
                session_id=f"session-{index % session_count}",
                product_id=f"product-{index % 10_000}",
                category=categories[index % len(categories)],
                affinity=((index * 37) % 1000) / 1000,
            )
            for index in range(events)
        ]
    else:
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
        "workload": workload,
        "active_keys": active_keys or events,
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
    parser.add_argument("--workload", choices=("counter", "shopping"), default="counter")
    parser.add_argument("--active-keys", type=int, default=0)
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
        args.workload,
        args.active_keys,
    ))
