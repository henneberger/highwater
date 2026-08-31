from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from temporal_code import (
    Client,
    LatePolicy,
    StreamOptions,
    WindowAggregateSpec,
    WindowAggregation,
    wait_for_watermark,
    workflow,
)


@workflow.defn
class WindowSumWorkflow:
    @workflow.run
    async def run(self, window: dict) -> dict:
        return {
            "window_start": window["window_start"],
            "window_end": window["window_end"],
            "sum": window["sum"],
            "record_count": window["count"],
            "finalized": window["finalized"],
        }


@workflow.defn
class EventTimeGateWorkflow:
    @workflow.run
    async def run(self, stream: str, event_time: float) -> dict:
        await wait_for_watermark(stream, event_time)
        return {"stream": stream, "watermark_reached": event_time}


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    stream = f"measurements-{suffix}"
    schedule = f"window-sum-{suffix}"
    source = f"measurements-source-{suffix}"
    await client.create_stream(stream, options=StreamOptions(
        partitions=2,
        max_out_of_orderness=2,
        allowed_lateness=1,
        idle_timeout=30,
        alignment_max_drift=20,
        late_policy=LatePolicy.SIDE_OUTPUT,
    ))
    window_sum = WindowAggregateSpec(
        operator_id=schedule,
        stream=stream,
        workflow=WindowSumWorkflow,
        window_size=10,
        start_at=0,
        aggregation=WindowAggregation.SUM,
    )
    await client.deploy(window_sum)
    await client.deploy(window_sum)
    gate = await client.start_workflow(
        EventTimeGateWorkflow,
        stream,
        21,
        workflow_id=f"event-time-gate-{suffix}",
    )

    first_event = await client.publish_event(
        stream,
        12,
        event_time=12,
        partition=0,
        source_id=source,
        source_partition=0,
        source_offset=0,
    )
    duplicate = await client.publish_event(
        stream,
        12,
        event_time=12,
        partition=0,
        source_id=source,
        source_partition=0,
        source_offset=0,
    )
    lease = next(
        lease for lease in await client.key_groups()
        if lease["key_group"] == first_event["record"]["key_group"]
    )
    reassigned = await client.assign_key_group(
        lease["key_group"],
        lease["owner"],
        expected_epoch=lease["epoch"],
    )
    second_event = await client.publish_event(
        stream,
        11,
        event_time=11,
        partition=0,
        source_id=source,
        source_partition=0,
        source_offset=1,
    )
    await client.publish_event(
        stream,
        4,
        event_time=4,
        partition=1,
        source_id=source,
        source_partition=1,
        source_offset=0,
    )
    await client.publish_event(
        stream,
        3,
        event_time=3,
        partition=1,
        source_id=source,
        source_partition=1,
        source_offset=1,
    )
    await client.advance_watermark(stream, 0, 21)
    info = await client.advance_watermark(stream, 1, 21)

    first_window = client.get_workflow_handle(f"stream/{schedule}/0.000000-10.000000")
    second_window = client.get_workflow_handle(f"stream/{schedule}/10.000000-20.000000")
    too_late = await client.publish_event(
        stream,
        99,
        event_time=5,
        partition=0,
        source_id=source,
        source_partition=0,
        source_offset=2,
    )
    await client.seal_partition(stream, 0)
    final_info = await client.seal_partition(stream, 1)

    print(json.dumps({
        "watermark": info.watermark,
        "finalized": final_info.finalized,
        "windows": [
            await first_window.result(timeout=10),
            await second_window.result(timeout=10),
        ],
        "event_time_gate": await gate.result(timeout=10),
        "source_retry_disposition": duplicate["disposition"],
        "key_group": first_event["record"]["key_group"],
        "owner_epochs": [
            first_event["record"]["owner_epoch"],
            second_event["record"]["owner_epoch"],
        ],
        "reassigned_epoch": reassigned["epoch"],
        "too_late_disposition": too_late["disposition"],
        "late_side_output": [record.value for record in await client.read_late_stream(stream)],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
