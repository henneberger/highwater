from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from dataclasses import dataclass, field
from time import monotonic

from highwater import (
    ActivityOptions,
    Client,
    NonRetryableError,
    ProcessOptions,
    RetryPolicy,
    StreamOptions,
    TemporalJoinType,
    WatermarkMode,
    current_activity,
    execute_activity,
    streaming,
    workflow,
)
from highwater.dag import Dag


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    customer_id: str
    kind: str
    occurred_at: float
    sku: str | None = None
    quantity: int = 0
    unit_price: int = 0
    payment_reference: str | None = None
    address: str | None = None


@streaming.process(key="customer_id", event_time="occurred_at", build_id="order-intake-v2")
@dataclass
class OrderIntake:
    lines: list[dict] = field(default_factory=list)
    total: int = 0
    submitted: bool = False

    @streaming.event
    async def apply(self, event: OrderEvent):
        if event.kind == "add_item":
            if event.sku is None or event.quantity <= 0 or event.unit_price <= 0:
                raise ValueError("an item requires a SKU, quantity, and unit price")
            self.lines.append(
                {
                    "sku": event.sku,
                    "quantity": event.quantity,
                    "unit_price": event.unit_price,
                }
            )
            self.total += event.quantity * event.unit_price
            return None
        if event.kind != "submit":
            raise ValueError(f"unknown order event: {event.kind}")
        if not self.lines or event.payment_reference is None or event.address is None:
            raise ValueError("a submitted order requires items, payment, and an address")
        self.submitted = True
        return {
            "status": "ready",
            "order_id": event.order_id,
            "customer_id": event.customer_id,
            "lines": self.lines,
            "total": self.total,
            "payment_reference": event.payment_reference,
            "address": event.address,
        }


FULFILLMENT_RETRY = ActivityOptions(
    task_queue="orders",
    retry_policy=RetryPolicy(
        maximum_attempts=4,
        initial_interval=0.05,
        backoff_coefficient=2,
        maximum_interval=1,
    ),
    schedule_to_close_timeout=10,
    start_to_close_timeout=2,
)


@streaming.task
def fulfill_order(order: dict, customer: dict) -> dict:
    attempt = current_activity().attempt
    if attempt == 1:
        raise RuntimeError("fulfillment service returned a transient 503")
    if not order["lines"]:
        raise NonRetryableError("cannot fulfill an empty order")
    order_id = order["order_id"]
    return {
        "order_id": order_id,
        "status": "shipped",
        "customer_version": customer["version"],
        "reservation_id": f"inventory:{order_id}",
        "charge_id": f"charge:{order_id}",
        "charged": order["total"],
        "tracking": f"tracking:{order['order_id']}",
        "address": order["address"],
        "attempts": attempt,
    }


@workflow.defn
class FulfillReadyOrder:
    @workflow.run
    async def run(self, joined: dict) -> dict:
        order = joined["probe"]["value"]
        version = joined["version"]
        if version is None:
            return {
                "order_id": order["order_id"],
                "status": "rejected",
                "reason": "no customer profile existed at submission time",
            }
        customer = version["value"]
        conditions = {
            "customer_active": customer["active"],
            "within_order_limit": order["total"] <= customer["order_limit"],
        }
        if not all(conditions.values()):
            return {
                "order_id": order["order_id"],
                "status": "rejected",
                "conditions": conditions,
                "customer_version": customer["version"],
            }
        result = await execute_activity(
            fulfill_order,
            order,
            customer,
            options=FULFILLMENT_RETRY,
        )
        result["conditions"] = conditions
        result["profile_effective_at"] = version["event_time"]
        result["order_event_time"] = joined["as_of"]
        return result


async def run_demo(target: str) -> dict:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    intake_id = f"order-intake-{suffix}"
    ready_stream = f"orders-ready-{suffix}"
    customer_profiles = f"customer-profiles-{suffix}"
    fulfillment_id = f"orders-at-customer-version-{suffix}"

    input_stream = f"{intake_id}-input"
    source_managed = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED)
    dag = (
        Dag("durable-order-pipeline")
        .stream(input_stream)
        .stream(ready_stream, source_managed)
        .stream(customer_profiles, source_managed)
        .process(
            OrderIntake,
            input=input_stream,
            process_id=intake_id,
            options=ProcessOptions(key="customer_id", task_queue="orders"),
            output=ready_stream,
        )
        .temporal_join(
            fulfillment_id,
            probe=ready_stream,
            versions=customer_profiles,
            workflow=FulfillReadyOrder,
            task_queue="orders",
            join_type=TemporalJoinType.LEFT,
        )
    )
    await dag.deploy(client)
    intake = await client.get_process_handle(intake_id)
    intake.owns_input = True

    order_id = f"order-{suffix}"
    customer_id = f"customer-{suffix}"
    await client.publish_event(
        customer_profiles,
        {
            "version": "standard-v1",
            "active": True,
            "tier": "standard",
            "order_limit": 5_000,
        },
        event_time=0,
        key=customer_id,
        event_id=f"{customer_id}:v1",
    )
    await client.publish_event(
        customer_profiles,
        {
            "version": "premium-v2",
            "active": True,
            "tier": "premium",
            "order_limit": 10_000,
        },
        event_time=4,
        key=customer_id,
        event_id=f"{customer_id}:v2",
    )
    events = [
        OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="add_item",
            occurred_at=1,
            sku="coffee",
            quantity=2,
            unit_price=1_200,
        ),
        OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="add_item",
            occurred_at=2,
            sku="filters",
            quantity=1,
            unit_price=800,
        ),
        OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="submit",
            occurred_at=3,
            payment_reference="payment-demo-4242",
            address="1 Highwater Way",
        ),
    ]
    for index, event in enumerate(events):
        await intake.send(event, event_id=f"{order_id}:{index}")
    await intake.finish(timeout=15)
    await client.advance_watermark(customer_profiles, 0, 10)

    deadline = monotonic() + 15
    outputs = []
    while monotonic() < deadline:
        outputs = await client.read_temporal_join(fulfillment_id)
        if outputs:
            break
        await asyncio.sleep(0.05)
    if not outputs:
        raise TimeoutError("temporal fulfillment stage did not receive the ready order")
    result = await client.get_workflow_handle(outputs[0].workflow_id).result(timeout=15)
    if "customer_version" not in result:
        raise RuntimeError(f"temporal join did not select a customer profile: {result}")
    return {
        "intake_state": await intake.state(customer_id),
        "fulfillment": result,
        "temporal_join_selected": result["customer_version"],
        "later_profile_version": "premium-v2",
        "fulfillment_retry_observed": result["attempts"] == 2,
    }


async def main(target: str) -> None:
    print(json.dumps(await run_demo(target), indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
