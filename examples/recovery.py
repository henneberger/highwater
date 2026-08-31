from __future__ import annotations

import argparse
import asyncio
import json

from temporal_code import Client
from examples.order import OrderWorkflow


async def main(target: str, finish: bool) -> None:
    client = Client(target)
    if not finish:
        await client.start_workflow(
            OrderWorkflow,
            "4242424242424242",
            25,
            workflow_id="recoverable-order",
        )
        checkpoint = await client.create_checkpoint()
        print(json.dumps({
            "workflow_id": "recoverable-order",
            "checkpoint": checkpoint,
            "next": "stop the server, remove or move its local state directory, restart it, then run with --finish",
        }, indent=2, sort_keys=True))
        return
    recovered = client.get_workflow_handle("recoverable-order")
    await recovered.signal("approve")
    print(json.dumps({
        "checkpoint": await client.checkpoint_manifest(),
        "recovered_result": await recovered.result(timeout=10),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--finish", action="store_true")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target, arguments.finish))
