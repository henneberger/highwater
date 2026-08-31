from __future__ import annotations

from highwater import (
    ActivityError,
    ActivityOptions,
    NonRetryableError,
    RetryPolicy,
    activity,
    execute_activity,
    heartbeat,
    workflow,
)


@activity.defn
def reserve_inventory(sku: str, quantity: int) -> dict:
    if quantity <= 0:
        raise NonRetryableError("quantity must be positive")
    for completed in range(quantity):
        heartbeat({"sku": sku, "reserved": completed + 1})
    return {"sku": sku, "quantity": quantity}


@activity.defn
def release_inventory(reservation: dict) -> None:
    heartbeat({"releasing": reservation})


@activity.defn
def create_shipment(reservation: dict, address: str) -> dict:
    if not address.strip():
        raise NonRetryableError("address is required")
    return {"tracking": f"shipment-{reservation['sku']}", "address": address}


RELIABLE_ACTIVITY = ActivityOptions(
    task_queue="orders",
    retry_policy=RetryPolicy(
        maximum_attempts=5,
        initial_interval=0.25,
        backoff_coefficient=2,
        maximum_interval=5,
    ),
    schedule_to_close_timeout=30,
    start_to_close_timeout=10,
    heartbeat_timeout=2,
)


@workflow.defn
class FulfillmentWorkflow:
    @workflow.run
    async def run(self, sku: str, quantity: int, address: str) -> dict:
        reservation = await execute_activity(
            reserve_inventory,
            sku,
            quantity,
            options=RELIABLE_ACTIVITY,
        )
        try:
            shipment = await execute_activity(
                create_shipment,
                reservation,
                address,
                options=RELIABLE_ACTIVITY,
            )
        except ActivityError:
            await execute_activity(
                release_inventory,
                reservation,
                options=RELIABLE_ACTIVITY,
            )
            raise
        return {"reservation": reservation, "shipment": shipment}
