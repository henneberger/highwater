from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import asdict

from highwater import (
    Client,
    ProcessOptions,
    StreamOptions,
    TemporalJoinType,
    WatermarkMode,
)
from highwater.dag import Dag

from examples.durable_order_pipeline import FulfillReadyOrder, OrderEvent, OrderIntake


ORDER_EVENTS = "orders-ingress"
READY_ORDERS = "orders-ready"
CUSTOMER_PROFILES = "customer-profile-versions"
ORDER_INTAKE = "order-intake"
ORDER_ENRICHMENT = "orders-at-profile-version"
SOURCE_ID = "continuous-order-source-v1"
EVENT_TIME_EPOCH = 1_788_231_000.0


def topology() -> Dag:
    options = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED)
    return (
        Dag("continuous-order-enrichment")
        .stream(ORDER_EVENTS, options)
        .stream(READY_ORDERS, options)
        .stream(CUSTOMER_PROFILES, options)
        .process(
            OrderIntake,
            input=ORDER_EVENTS,
            process_id=ORDER_INTAKE,
            options=ProcessOptions(key="customer_id", task_queue="orders"),
            output=READY_ORDERS,
        )
        .temporal_join(
            ORDER_ENRICHMENT,
            probe=READY_ORDERS,
            versions=CUSTOMER_PROFILES,
            workflow=FulfillReadyOrder,
            task_queue="orders",
            join_type=TemporalJoinType.LEFT,
        )
    )


async def deploy(client: Client) -> None:
    await topology().deploy(client)


def event_for_offset(offset: int, interval: float) -> tuple[OrderEvent, dict, str, float]:
    order_number, stage = divmod(offset, 3)
    order_id = f"continuous-order-{order_number:012d}"
    customer_id = f"continuous-customer-{order_number:012d}"
    base_time = EVENT_TIME_EPOCH + order_number * interval
    occurred_at = base_time + interval * (stage + 1) / 4
    profile = {
        "version": f"profile-{order_number:012d}",
        "active": True,
        "tier": "premium" if order_number % 3 == 0 else "standard",
        "order_limit": 10_000,
    }
    if stage == 0:
        event = OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="add_item",
            occurred_at=occurred_at,
            sku="coffee",
            quantity=2,
            unit_price=1_200,
        )
    elif stage == 1:
        event = OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="add_item",
            occurred_at=occurred_at,
            sku="filters",
            quantity=1,
            unit_price=800,
        )
    else:
        event = OrderEvent(
            order_id=order_id,
            customer_id=customer_id,
            kind="submit",
            occurred_at=occurred_at,
            payment_reference="payment-example-4242",
            address="1 Highwater Way",
        )
    return event, profile, customer_id, base_time


async def run_session(client: Client, interval: float) -> None:
    writer = client.stream_writer(ORDER_EVENTS, source_id=SOURCE_ID)
    async with writer:
        while True:
            event, profile, customer_id, profile_event_time = event_for_offset(
                writer.next_offset, interval,
            )
            await client.publish_event(
                CUSTOMER_PROFILES,
                profile,
                event_time=profile_event_time,
                key=customer_id,
                event_id=f"{customer_id}:profile",
            )
            await writer.publish(
                asdict(event),
                event_time=event.occurred_at,
                key=customer_id,
            )
            await client.advance_watermark(CUSTOMER_PROFILES, 0, event.occurred_at)
            await writer.advance_watermark(event.occurred_at)
            await asyncio.sleep(max(0.25, interval / 3))


async def run_forever(target: str, interval: float) -> None:
    delay = 1.0
    while True:
        try:
            client = Client(target)
            await deploy(client)
            delay = 1.0
            await run_session(client, interval)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"continuous source reconnecting: {error}", file=sys.stderr, flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        default=os.environ.get("HIGHWATER_ADDRESS", "http://127.0.0.1:7233"),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("HIGHWATER_LIVE_INTERVAL", "15")),
    )
    arguments = parser.parse_args()
    if arguments.interval <= 0:
        parser.error("--interval must be positive")
    asyncio.run(run_forever(arguments.target, arguments.interval))


if __name__ == "__main__":
    main()
