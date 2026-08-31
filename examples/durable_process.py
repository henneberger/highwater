from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from dataclasses import dataclass

from highwater import Client, streaming


@dataclass(frozen=True)
class AccountEvent:
    account_id: str
    amount: int
    occurred_at: float


@streaming.process(
    key="account_id",
    event_time="occurred_at",
    wait_until=streaming.complete,
    state_version=1,
)
@dataclass
class AccountBalanceProcess:
    balance: int = 0
    events: int = 0
    complete_through: float = 0

    @streaming.event
    async def apply(self, event: AccountEvent):
        self.balance += event.amount
        self.events += 1
        self.complete_through = event.occurred_at
        return {
            "balance": self.balance,
            "events": self.events,
            "complete_through": self.complete_through,
        }


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    balances = f"account-balances-{suffix}"

    accounts = await client.start(
        AccountBalanceProcess,
        process_id=balances,
    )

    await accounts.send(AccountEvent("account-a", 5, 10))
    await accounts.send(AccountEvent("account-a", 7, 12))
    await accounts.send(AccountEvent("account-b", 3, 11))
    await accounts.finish(timeout=10)

    print(json.dumps({
        "account_a": await accounts.state("account-a"),
        "account_b": await accounts.state("account-b"),
        "process": await client.process(balances),
        "state_changelog": await client.read_operator_changes(balances),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
