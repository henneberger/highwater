"""A native retractable leaderboard; run against highwater-server on localhost:7233."""
from __future__ import annotations

import asyncio

from highwater import Client, CollectionMode, StreamOptions, WatermarkMode
from highwater.dag import Dag


def topology() -> Dag:
    managed = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED, idle_timeout=None)
    return (
        Dag("leaderboard")
        .stream("leaderboard-scores", managed)
        .stream("leaderboard-changes", managed)
        .top_n("leaderboard", input="leaderboard-scores", n=2,
               primary_key=["player"], partition_by=["league"],
               order_by=[("score", "desc"), ("player", "asc")],
               input_mode=CollectionMode.UPSERT, output="leaderboard-changes")
    )


async def main() -> None:
    client = Client()
    await topology().deploy(client)
    for index, (player, score) in enumerate((("Alice", 100), ("Bob", 90), ("Carol", 80))):
        await client.publish_event("leaderboard-scores", {"player": player, "league": "west", "score": score},
                                   event_time=10 + index, kind="upsert")
    print("Before:", await client.collection_rows("leaderboard", group=["west"]))
    await client.publish_event("leaderboard-scores", {"player": "Alice"}, event_time=20, kind="delete")
    rows = await client.collection_rows("leaderboard", group=["west"])
    assert {row["value"]["player"] for row in rows} == {"Bob", "Carol"}
    print("After deleting Alice:", rows)


if __name__ == "__main__":
    asyncio.run(main())
