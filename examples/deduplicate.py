from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from highwater import Client, StreamOptions, workflow
from highwater.dag import Dag


@workflow.defn
class DeduplicateWorkflow:
    @workflow.run
    async def run(self, record: dict) -> dict:
        return {
            "key": record["key"],
            "event_time": record["event_time"],
            "value": record["value"],
        }


def topology(stream: str, operator_id: str) -> Dag:
    return (
        Dag("deduplicate")
        .stream(stream, StreamOptions(
            max_out_of_orderness=5,
            allowed_lateness=1,
        ))
        .deduplicate(
            operator_id,
            input=stream,
            workflow=DeduplicateWorkflow,
        )
    )


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    stream = f"commands-{suffix}"
    operator_id = f"first-command-{suffix}"
    await topology(stream, operator_id).deploy(client)
    await topology(stream, operator_id).deploy(client)

    rows = [
        ("a-arrived-first", "a", 10),
        ("a-earliest-event-time", "a", 7),
        ("b-canonical", "b", 9),
        ("a-later-duplicate", "a", 11),
    ]
    for event_id, key, event_time in rows:
        await client.publish_event(
            stream,
            {"command_id": event_id},
            event_time=event_time,
            key=key,
            event_id=event_id,
        )

    await client.advance_watermark(stream, 0, 12)
    outputs = await client.read_deduplicate(operator_id)
    emitted = [
        await client.get_workflow_handle(output.workflow_id).result(timeout=10)
        for output in outputs
        if output.workflow_id is not None
    ]
    await client.seal_partition(stream, 0)

    print(json.dumps({
        "operator": await client.deduplicate(operator_id),
        "emitted": emitted,
        "suppressed": [
            output.record.value
            for output in outputs
            if not output.canonical
        ],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
