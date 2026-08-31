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
    Comparison,
    FilterSpec,
    NonRetryableError,
    ProcessOptions,
    RetryPolicy,
    StreamOptions,
    WatermarkMode,
    activity,
    current_activity,
    execute_activity,
    streaming,
    workflow,
)


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    kind: str
    occurred_at: float
    sku: str | None = None
    quantity: int = 0
    unit_price: int = 0
    payment_reference: str | None = None
    address: str | None = None


@streaming.process(key="order_id", event_time="occurred_at", build_id="order-intake-v1")
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
            "lines": self.lines,
            "total": self.total,
            "payment_reference": event.payment_reference,
            "address": event.address,
        }


RETRY_TRANSIENT_IO = ActivityOptions(
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


@activity.defn
def reserve_order_inventory(order: dict) -> dict:
    attempt = current_activity().attempt
    if attempt == 1:
        raise RuntimeError("inventory service returned a transient 503")
    if not order["lines"]:
        raise NonRetryableError("cannot reserve an empty order")
    return {
        "reservation_id": f"inventory:{order['order_id']}",
        "lines": order["lines"],
        "attempts": attempt,
    }


@activity.defn
def capture_payment(order: dict) -> dict:
    return {
        "charge_id": f"charge:{order['order_id']}",
        "payment_reference": order["payment_reference"],
        "amount": order["total"],
    }


@activity.defn
def create_shipping_label(order: dict, reservation: dict) -> dict:
    return {
        "tracking": f"tracking:{order['order_id']}",
        "address": order["address"],
        "reservation_id": reservation["reservation_id"],
    }


@workflow.defn
class FulfillReadyOrder:
    @workflow.run
    async def run(self, record: dict) -> dict:
        order = record["value"]
        reservation = await execute_activity(
            reserve_order_inventory,
            order,
            options=RETRY_TRANSIENT_IO,
        )
        payment = await execute_activity(
            capture_payment,
            order,
            options=RETRY_TRANSIENT_IO,
        )
        shipment = await execute_activity(
            create_shipping_label,
            order,
            reservation,
            options=RETRY_TRANSIENT_IO,
        )
        return {
            "order_id": order["order_id"],
            "status": "shipped",
            "reservation": reservation,
            "payment": payment,
            "shipment": shipment,
        }


async def run_demo(target: str) -> dict:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    intake_id = f"order-intake-{suffix}"
    ready_stream = f"orders-ready-{suffix}"
    fulfillment_id = f"fulfill-orders-{suffix}"

    intake = await client.start(
        OrderIntake,
        process_id=intake_id,
        options=ProcessOptions(key="order_id", task_queue="orders"),
    )
    await client.create_stream(
        ready_stream,
        options=StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED),
    )
    await client.deploy(
        FilterSpec(
            operator_id=fulfillment_id,
            stream=ready_stream,
            workflow=FulfillReadyOrder,
            field="status",
            comparison=Comparison.EQUAL,
            operand="ready",
            task_queue="orders",
        )
    )
    await client.connect_operator(intake_id, ready_stream)

    order_id = f"order-{suffix}"
    events = [
        OrderEvent(order_id, "add_item", 1, "coffee", 2, 1_200),
        OrderEvent(order_id, "add_item", 2, "filters", 1, 800),
        OrderEvent(
            order_id,
            "submit",
            3,
            payment_reference="payment-demo-4242",
            address="1 Highwater Way",
        ),
    ]
    for index, event in enumerate(events):
        await intake.send(event, event_id=f"{order_id}:{index}")
    await intake.finish(timeout=15)

    deadline = monotonic() + 15
    outputs = []
    while monotonic() < deadline:
        outputs = await client.read_stream_filter(fulfillment_id)
        if outputs:
            break
        await asyncio.sleep(0.05)
    if not outputs:
        raise TimeoutError("fulfillment stage did not receive the ready order")
    result = await client.get_workflow_handle(outputs[0]["workflow_id"]).result(timeout=15)
    return {
        "intake_state": await intake.state(order_id),
        "fulfillment": result,
        "inventory_retry_observed": result["reservation"]["attempts"] == 2,
    }


async def main(target: str) -> None:
    print(json.dumps(await run_demo(target), indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
