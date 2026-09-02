"""Inspectable deployment DAGs for the representative examples."""

from __future__ import annotations

from highwater import StreamOptions
from highwater.dag import Dag

from examples.ai_support_operations import topology as ai_support_operations
from examples.batched_embeddings import BatchedEmbeddings
from examples.clickstream_recommendation import topology as clickstream_topology
from examples.continuous_order_enrichment import topology as order_topology
from examples.deduplicate import topology as deduplicate_topology
from examples.event_time_windows import topology as windows_topology
from examples.iot_sensor_metrics import topology as iot_topology
from examples.shopping_assistant import ShoppingAssistant


def shopping_assistant() -> Dag:
    return (
        Dag("shopping-assistant")
        .stream(
            "catalog",
            StreamOptions(max_out_of_orderness=0, allowed_lateness=0),
        )
        .process(
            ShoppingAssistant,
            input="shopping-assistant-input",
            process_id="shopping-assistant",
        )
    )


def batched_embeddings() -> Dag:
    return Dag("batched-embeddings").process(
        BatchedEmbeddings,
        input="embeddings-input",
        process_id="embeddings",
    )


def clickstream_recommendation() -> Dag:
    return clickstream_topology(
        "visits",
        "content-clicks",
        "content-versions",
        "co-visits",
        "content-at-click",
    )


def iot_sensor_metrics() -> Dag:
    return iot_topology(
        "sensor-readings",
        "high-temperature",
        "sensor-sliding-max",
        "alert-changes",
    )


def event_time_windows() -> Dag:
    return windows_topology("measurements", "window-sum")


def deduplicate() -> Dag:
    return deduplicate_topology("commands", "first-command")


def continuous_order_enrichment() -> Dag:
    return order_topology()


ALL = {
    "ai-support-operations": ai_support_operations,
    "batched-embeddings": batched_embeddings,
    "clickstream-recommendation": clickstream_recommendation,
    "continuous-order-enrichment": continuous_order_enrichment,
    "deduplicate": deduplicate,
    "event-time-windows": event_time_windows,
    "iot-sensor-metrics": iot_sensor_metrics,
    "shopping-assistant": shopping_assistant,
}
