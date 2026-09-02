from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field

from highwater import Client, Registry, StreamOptions, streaming
from highwater.rust_worker import RustWorker


class RecommendationModel:
    async def rank(self, *, product: dict, recent: list[str]) -> dict:
        await asyncio.sleep(0.001)
        return {
            "product_id": product.product_id,
            "category": product.category,
            "recent": recent,
        }


recommendation_model = RecommendationModel()
catalog = streaming.versioned("catalog", key="product_id")


@streaming.process(key="user_id")
@dataclass
class ShoppingAssistant:
    recent: list[str] = field(default_factory=list)

    @streaming.event
    async def recommend(self, view, context):
        product = await catalog.get(
            view.product_id, as_of=context.event_time
        )
        if product is None:
            return None
        self.recent.append(product.category)
        self.recent = self.recent[-5:]
        return await recommendation_model.rank(
            product=product, recent=self.recent
        )


async def main(target: str) -> None:
    client = Client(target)
    try:
        await client.create_stream(
            "catalog",
            options=StreamOptions(max_out_of_orderness=0, allowed_lateness=0),
        )
    except RuntimeError as error:
        if "already exists" not in str(error):
            raise
    handle = await client.start(ShoppingAssistant, process_id="shopping-assistant")
    registry = Registry()
    registry.register_workflow(ShoppingAssistant)
    worker = RustWorker(
        registry,
        target=target,
    )
    worker_task = asyncio.create_task(worker.run_forever())
    try:
        await client.publish_event(
            "catalog",
            {"product_id": "book-7", "category": "books"},
            key="book-7",
            event_time=5,
        )
        await handle.send(
            {"user_id": "user-1", "product_id": "book-7"},
            event_time=10,
        )
        await client.advance_watermark("catalog", 0, 10)
        drain_task = asyncio.create_task(handle.drain(timeout=10))
        done, _ = await asyncio.wait(
            {worker_task, drain_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if worker_task in done:
            worker_task.result()
        await drain_task
        print(json.dumps(await handle.state("user-1"), indent=2))
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
