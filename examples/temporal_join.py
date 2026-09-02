from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass

from highwater import ChangeKind, Client, Registry, StreamOptions, streaming
from highwater.rust_worker import RustWorker


accounts = streaming.versioned("account-versions", key="account_id")


@streaming.process(key="account_id")
@dataclass
class PurchaseCheck:
    checked: int = 0

    @streaming.event
    async def check(self, purchase, context):
        account = await accounts.get(
            purchase.account_id, as_of=context.event_time
        )
        self.checked += 1
        if account is None:
            return {
                "purchase_id": purchase.purchase_id,
                "matched": False,
                "reason": "no account version existed at purchase time",
            }

        conditions = {
            "country_matches": account.country == purchase.country,
            "account_active": account.active,
            "minimum_amount": purchase.amount >= 100,
        }
        return {
            "purchase_id": purchase.purchase_id,
            "account_version": account.version,
            "purchase_time": context.event_time,
            "conditions": conditions,
            "matched": all(conditions.values()),
        }


async def ensure_stream(client: Client, name: str) -> None:
    try:
        await client.create_stream(
            name,
            options=StreamOptions(max_out_of_orderness=100, allowed_lateness=1),
        )
    except RuntimeError as error:
        if "already exists" not in str(error):
            raise


async def main(target: str) -> None:
    client = Client(target)
    await ensure_stream(client, "purchases")
    await ensure_stream(client, "account-versions")
    checks = await client.start(
        PurchaseCheck,
        source="purchases",
        process_id="purchase-checks",
    )

    registry = Registry()
    registry.register_workflow(PurchaseCheck)
    worker = RustWorker(registry, target=target)
    worker_task = asyncio.create_task(worker.run_forever())
    try:
        for event_id, account_id, value, event_time, kind in (
            ("account-a-v1", "a", {"version": "a-v1", "country": "US", "active": True}, 5, ChangeKind.UPSERT),
            ("account-b-v1", "b", {"version": "b-v1", "country": "GB", "active": True}, 9, ChangeKind.UPSERT),
            ("account-a-v2", "a", {"version": "a-v2", "country": "CA", "active": False}, 12, ChangeKind.UPSERT),
            ("account-c-deleted", "c", None, 11, ChangeKind.DELETE),
        ):
            await client.publish_event(
                "account-versions",
                value,
                key=account_id,
                event_time=event_time,
                event_id=event_id,
                kind=kind,
            )

        purchases = (
            ("purchase-before-update", "a", "US", 150, 10),
            ("purchase-after-update", "a", "US", 150, 14),
            ("purchase-b", "b", "GB", 110, 13),
            ("purchase-low", "b", "GB", 80, 14),
            ("purchase-deleted", "c", "US", 120, 13),
        )
        for purchase_id, account_id, country, amount, event_time in purchases:
            await client.publish_event(
                "purchases",
                {
                    "purchase_id": purchase_id,
                    "account_id": account_id,
                    "country": country,
                    "amount": amount,
                },
                key=account_id,
                event_time=event_time,
                event_id=purchase_id,
            )

        await client.advance_watermark("account-versions", 0, 20)
        await checks.drain(timeout=10)
        changes = await client.read_operator_changes("purchase-checks")
        print(json.dumps(
            sorted(
                (change["row"] for change in changes if change["diff"] > 0),
                key=lambda row: row["purchase_id"],
            ),
            indent=2,
            sort_keys=True,
        ))
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
