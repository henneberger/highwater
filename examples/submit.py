from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from temporal_code import Client, WorkflowOptions


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]

    order = await client.start_workflow(
        "OrderWorkflow", "4242424242424242", 25, workflow_id=f"order-{suffix}",
    )
    await order.update("change_amount", 30)
    await order.signal("approve")

    aggregation = await client.start_workflow(
        "PagedAggregationWorkflow", [[1, 2], [3, 4], [5]], workflow_id=f"aggregation-{suffix}",
    )
    fulfillment = await client.start_workflow(
        "FulfillmentWorkflow",
        "sku-123",
        3,
        "1 Temporal Way",
        workflow_id=f"fulfillment-{suffix}",
        options=WorkflowOptions(task_queue="orders", execution_timeout=30),
    )

    results = {
        "order": await order.result(timeout=10),
        "aggregation": await aggregation.result(timeout=10),
        "fulfillment": await fulfillment.result(timeout=10),
    }
    consumer_id = f"example-sink-{suffix}"
    delivered = []
    for _ in range(3):
        message = await client.poll_sink("workflows", consumer_id)
        if message is None:
            break
        await client.ack_sink("workflows", message["message_id"], consumer_id)
        delivered.append({
            "message_id": message["message_id"],
            "workflow_id": message["workflow_id"],
            "delivery_attempt": message["delivery_attempt"],
        })
    print(json.dumps({
        "results": results,
        "acknowledged_outbox_messages": delivered,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
