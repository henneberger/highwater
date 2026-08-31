from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from temporal_code import ChangeKind, Client, IntervalJoinSpec, StreamOptions, workflow


@workflow.defn
class IntervalJoinWorkflow:
    @workflow.run
    async def run(self, joined: dict) -> dict:
        login = joined["left"]["value"]
        purchase = joined["right"]["value"]
        conditions = {
            "country_matches": login["country"] == purchase["country"],
            "minimum_amount": purchase["amount"] >= 100,
        }
        return {
            "login_id": login["login_id"],
            "purchase_id": purchase["purchase_id"],
            "event_time_delta": (
                joined["right"]["event_time"] - joined["left"]["event_time"]
            ),
            "conditions": conditions,
            "matched": all(conditions.values()),
        }


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    logins = f"logins-{suffix}"
    purchases = f"interval-purchases-{suffix}"
    join_id = f"login-purchases-{suffix}"
    options = StreamOptions(max_out_of_orderness=2, allowed_lateness=1)
    await client.create_stream(logins, options=options)
    await client.create_stream(purchases, options=options)
    join = IntervalJoinSpec(
        operator_id=join_id,
        left_stream=logins,
        right_stream=purchases,
        workflow=IntervalJoinWorkflow,
        lower_bound=0,
        upper_bound=5,
    )
    await client.deploy(join)
    await client.deploy(join)

    await client.publish_event(logins, {
        "login_id": "login-a",
        "country": "US",
    }, event_time=10, key="a", event_id="login-a")
    await client.publish_event(logins, {
        "login_id": "login-b",
        "country": "GB",
    }, event_time=20, key="b", event_id="login-b")
    rows = [
        ("purchase-a", "a", "US", 150, 12),
        ("purchase-country", "a", "CA", 120, 14),
        ("purchase-outside", "a", "US", 200, 17),
        ("purchase-low", "b", "GB", 80, 23),
    ]
    for purchase_id, account_id, country, amount, event_time in rows:
        await client.publish_event(purchases, {
            "purchase_id": purchase_id,
            "country": country,
            "amount": amount,
        }, event_time=event_time, key=account_id, event_id=purchase_id)
        if purchase_id == "purchase-a":
            await client.publish_event(purchases, {
                "purchase_id": "purchase-a",
                "country": "US",
                "amount": 150,
            }, event_time=12, key="a", kind=ChangeKind.DELETE,
                event_id="retract-purchase-a")

    outputs = await client.read_interval_join(join_id)
    results = [
        await client.get_workflow_handle(output.workflow_id).result(timeout=10)
        for output in outputs
    ]
    await client.advance_watermark(logins, 0, 30)
    await client.advance_watermark(purchases, 0, 30)
    await client.seal_partition(logins, 0)
    await client.seal_partition(purchases, 0)

    print(json.dumps({
        "join": await client.interval_join(join_id),
        "changelog": await client.read_operator_changes(join_id),
        "results": sorted(results, key=lambda result: result["purchase_id"]),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
