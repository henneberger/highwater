from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from temporal_code import (
    Client,
    IntervalJoinSpec,
    LatePolicy,
    StreamOptions,
    TemporalJoinSpec,
    TemporalJoinType,
    workflow,
)


@workflow.defn
class CoVisitRecommendationWorkflow:
    @workflow.run
    async def run(self, pair: dict) -> dict:
        return {
            "user_id": pair["left"]["key"],
            "url": pair["left"]["value"]["url"],
            "recommendation": pair["right"]["value"]["url"],
            "frequency_delta": 1,
            "event_time": pair["event_time"],
        }


@workflow.defn
class ContentAtClickWorkflow:
    @workflow.run
    async def run(self, joined: dict) -> dict:
        content = joined["version"]
        return {
            "user_id": joined["probe"]["value"]["user_id"],
            "url": joined["probe"]["key"],
            "event_time": joined["as_of"],
            "content": None if content is None else content["value"],
        }


async def main(target: str) -> None:
    client = Client(target)
    suffix = uuid.uuid4().hex[:8]
    visits = f"clickstream-{suffix}"
    content_clicks = f"content-clicks-{suffix}"
    content = f"content-versions-{suffix}"
    co_visits = f"co-visits-{suffix}"
    content_at_click = f"content-at-click-{suffix}"

    options = StreamOptions(allowed_lateness=5, late_policy=LatePolicy.SIDE_OUTPUT)
    for stream in (visits, content_clicks, content):
        await client.create_stream(stream, options=options)

    await client.deploy(IntervalJoinSpec(
        operator_id=co_visits,
        left_stream=visits,
        right_stream=visits,
        workflow=CoVisitRecommendationWorkflow,
        lower_bound=0,
        upper_bound=600,
    ))
    await client.deploy(TemporalJoinSpec(
        operator_id=content_at_click,
        probe_stream=content_clicks,
        version_stream=content,
        workflow=ContentAtClickWorkflow,
        join_type=TemporalJoinType.LEFT,
    ))

    content_rows = [
        ("/home", {"title": "Home", "embedding_ref": "vectors/home-v1"}),
        ("/products", {"title": "Products", "embedding_ref": "vectors/products-v1"}),
        ("/checkout", {"title": "Checkout", "embedding_ref": "vectors/checkout-v1"}),
    ]
    content_writer = client.stream_writer(
        content, source_id=f"content-cdc-{suffix}",
    )
    for url, value in content_rows:
        await content_writer.publish(
            value,
            event_time=0,
            key=url,
        )

    clicks = [
        ("user-1", "/home", 0),
        ("user-1", "/products", 120),
        ("user-2", "/home", 200),
        ("user-1", "/checkout", 500),
        ("user-2", "/products", 900),
    ]
    visit_writer = client.stream_writer(
        visits, source_id=f"click-source-{suffix}",
    )
    enrichment_writer = client.stream_writer(
        content_clicks, source_id=f"content-click-source-{suffix}",
    )
    for user_id, url, event_time in clicks:
        await visit_writer.publish(
            {"user_id": user_id, "url": url},
            event_time=event_time,
            key=user_id,
        )
        await enrichment_writer.publish(
            {"user_id": user_id, "url": url},
            event_time=event_time,
            key=url,
        )

    co_visit_outputs = await client.read_interval_join(co_visits)
    recommendation_updates = [
        await client.get_workflow_handle(output.workflow_id).result(timeout=10)
        for output in co_visit_outputs
    ]

    await client.advance_watermark(content_clicks, 0, 1_000)
    await client.advance_watermark(content, 0, 1_000)
    enriched_outputs = await client.read_temporal_join(content_at_click)
    enriched_clicks = [
        await client.get_workflow_handle(output.workflow_id).result(timeout=10)
        for output in enriched_outputs
        if output.workflow_id is not None
    ]

    print(json.dumps({
        "recommendation_updates": recommendation_updates,
        "content_at_click": enriched_clicks,
        "note": "frequency_delta is an incremental changelog; vector ranking is intentionally a future stateful operator",
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
