from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from temporal_code import (
    ChangeKind,
    Client,
    StreamOptions,
    TemporalJoinSpec,
    TemporalJoinType,
    workflow,
)


@workflow.defn
class TemporalJoinWorkflow:
    @workflow.run
    async def run(self, joined: dict) -> dict:
        purchase = joined["probe"]["value"]
        version_record = joined["version"]
        if version_record is None:
            return {
                "purchase_id": purchase["purchase_id"],
                "matched": False,
                "reason": "no account version existed at purchase time",
            }

        account = version_record["value"]
        conditions = {
            "country_matches": account["country"] == purchase["country"],
            "account_active": account["active"],
            "minimum_amount": purchase["amount"] >= 100,
        }
        return {
            "purchase_id": purchase["purchase_id"],
            "account_version": account["version"],
            "account_version_time": version_record["event_time"],
            "purchase_time": joined["as_of"],
            "conditions": conditions,
            "matched": all(conditions.values()),
        }


async def publish(
    client: Client,
    stream: str,
    event_id: str,
    key: str,
    value: dict | None,
    event_time: float,
    *,
    kind: ChangeKind = ChangeKind.UPSERT,
) -> None:
    await client.publish_event(
        stream,
        value,
        event_time=event_time,
        key=key,
        kind=kind,
        event_id=event_id,
    )


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    purchases = f"purchases-{suffix}"
    accounts = f"account-versions-{suffix}"
    join_id = f"purchase-accounts-{suffix}"
    stream_options = StreamOptions(
        max_out_of_orderness=100,
        allowed_lateness=1,
    )
    await client.create_stream(purchases, options=stream_options)
    await client.create_stream(accounts, options=stream_options)
    join = TemporalJoinSpec(
        operator_id=join_id,
        probe_stream=purchases,
        version_stream=accounts,
        workflow=TemporalJoinWorkflow,
        join_type=TemporalJoinType.LEFT,
    )
    await client.deploy(join)
    await client.deploy(join)

    await publish(client, accounts, "account-a-v1", "a", {
        "version": "a-v1",
        "country": "US",
        "active": True,
    }, 5)
    await publish(client, accounts, "account-b-v1", "b", {
        "version": "b-v1",
        "country": "GB",
        "active": True,
    }, 9)
    await publish(client, accounts, "account-a-v2", "a", {
        "version": "a-v2",
        "country": "CA",
        "active": False,
    }, 12)
    await publish(
        client,
        accounts,
        "account-c-deleted",
        "c",
        None,
        11,
        kind=ChangeKind.DELETE,
    )

    probe_rows = [
        ("purchase-before-update", "a", "US", 150, 10),
        ("purchase-no-fallback", "a", "US", 150, 14),
        ("purchase-b", "b", "GB", 110, 13),
        ("purchase-low", "b", "GB", 80, 14),
        ("purchase-deleted", "c", "US", 120, 13),
        ("purchase-cross-boundary", "b", "GB", 125, 31),
    ]
    for purchase_id, account_id, country, amount, event_time in probe_rows:
        await publish(client, purchases, purchase_id, account_id, {
            "purchase_id": purchase_id,
            "country": country,
            "amount": amount,
        }, event_time)

    await client.advance_watermark(accounts, 0, 11)
    await client.advance_watermark(purchases, 0, 11)
    first = client.get_workflow_handle(
        f"temporal-join/{join_id}/{purchases}/0000000000/00000000000000000000",
    )
    first_result = await first.result(timeout=10)

    await client.advance_watermark(accounts, 0, 32)
    await client.advance_watermark(purchases, 0, 32)
    outputs = await client.read_temporal_join(join_id)
    results = [first_result]
    for output in outputs:
        if output.workflow_id == first.id or output.workflow_id is None:
            continue
        results.append(
            await client.get_workflow_handle(output.workflow_id).result(timeout=10),
        )

    first_probe = probe_rows[0]
    duplicate = await client.publish_event(
        purchases,
        {
            "purchase_id": first_probe[0],
            "country": first_probe[2],
            "amount": first_probe[3],
        },
        event_time=first_probe[4],
        key=first_probe[1],
        event_id=first_probe[0],
    )
    await client.seal_partition(accounts, 0)
    await client.seal_partition(purchases, 0)

    print(json.dumps({
        "first_result_at_watermark_11": first_result,
        "all_results": sorted(results, key=lambda result: result["purchase_id"]),
        "duplicate_disposition": duplicate["disposition"],
        "join": await client.temporal_join(join_id),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
