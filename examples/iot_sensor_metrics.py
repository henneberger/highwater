from __future__ import annotations

import argparse
import asyncio
import base64
import json
import uuid

from highwater import (
    Client,
    Comparison,
    LatePolicy,
    StreamOptions,
    WatermarkMode,
    WindowAggregation,
    workflow,
)
from highwater.dag import Dag


@workflow.defn
class HighTemperatureAlertWorkflow:
    @workflow.run
    async def run(self, record: dict) -> dict:
        return {
            "sensor_id": record["key"],
            "temperature": record["value"]["temperature"],
            "event_time": record["event_time"],
            "severity": "high",
        }


@workflow.defn
class SensorWindowMaxWorkflow:
    @workflow.run
    async def run(self, window: dict) -> dict:
        return {
            "sensor_id": window["key"],
            "window_start": window["window_start"],
            "window_end": window["window_end"],
            "max_temperature": window["max"],
            "reading_count": window["count"],
        }


def encoded(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def topology(
    stream: str,
    alerts: str,
    sliding_max: str,
    alert_changes: str,
) -> Dag:
    return (
        Dag("iot-sensor-metrics")
        .stream(stream, StreamOptions(
            allowed_lateness=5,
            late_policy=LatePolicy.SIDE_OUTPUT,
        ))
        .stream(alert_changes, StreamOptions(
            watermark_mode=WatermarkMode.SOURCE_MANAGED,
        ))
        .filter(
            alerts,
            input=stream,
            workflow=HighTemperatureAlertWorkflow,
            field="temperature",
            comparison=Comparison.GREATER_THAN,
            operand=100,
            output=alert_changes,
        )
        .window(
            sliding_max,
            input=stream,
            workflow=SensorWindowMaxWorkflow,
            size=30,
            slide=10,
            start_at=0,
            aggregation=WindowAggregation.MAX,
            value="temperature",
        )
    )


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    stream = f"sensor-readings-{suffix}"
    alerts = f"high-temperature-{suffix}"
    sliding_max = f"sensor-sliding-max-{suffix}"
    source = f"iot-gateway-{suffix}"
    alert_changes = f"alert-changes-{suffix}"

    await topology(stream, alerts, sliding_max, alert_changes).deploy(client)

    readings = [
        ("sensor-a", 5, 90),
        ("sensor-b", 8, 70),
        ("sensor-a", 15, 105),
        ("sensor-b", 18, 120),
        ("sensor-a", 25, 98),
        ("sensor-c", 50, 50),
    ]
    writer = client.stream_writer(stream, source_id=source)
    for sensor_id, event_time, temperature in readings:
        await writer.publish(
            {"sensor_id": sensor_id, "temperature": temperature},
            event_time=event_time,
            key=sensor_id,
        )

    alert_outputs = await client.read_stream_filter(alerts)
    alert_results = [
        await client.get_workflow_handle(output["workflow_id"]).result(timeout=10)
        for output in alert_outputs
    ]
    for _ in range(20):
        edge = await client.operator_edge(alerts)
        if edge["changes_forwarded"] == len(alert_outputs):
            break
        await asyncio.sleep(0.05)

    info = await client.stream_info(stream)
    window_results = []
    for window_start in (0, 10):
        for sensor_id in ("sensor-a", "sensor-b"):
            workflow_id = (
                f"stream/{sliding_max}/{encoded(sensor_id)}/"
                f"{window_start:.6f}-{window_start + 30:.6f}"
            )
            window_results.append(
                await client.get_workflow_handle(workflow_id).result(timeout=10)
            )

    print(json.dumps({
        "alerts": alert_results,
        "durable_alert_changelog": [
            record.value for record in await client.read_stream(alert_changes)
        ],
        "sliding_maxima": window_results,
        "watermark_diagnostics": info.watermark_diagnostics,
        "differential_changes": await client.read_operator_changes(sliding_max),
        "operator": await client.window_schedule(sliding_max),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
