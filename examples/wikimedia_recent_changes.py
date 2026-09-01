from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import BinaryIO
from urllib.request import Request, urlopen

from highwater import Client, ProcessOptions, StreamOptions, WatermarkMode, streaming


FEED_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
INPUT_STREAM = "wikimedia-recent-changes"
PROCESS_ID = "wikimedia-page-activity"
SOURCE_ID = "wikimedia-eventstreams-v1"
USER_AGENT = "HighwaterDemo/0.0.2 (https://highwater.cloud)"


@dataclass(frozen=True)
class RecentChange:
    event_id: str
    page_key: str
    wiki: str
    title: str
    change_type: str
    occurred_at: float
    length_delta: int
    bot: bool
    url: str | None


@streaming.process(
    key="page_key",
    event_time="occurred_at",
    build_id="wikimedia-page-activity-v1",
)
@dataclass
class WikimediaPageActivity:
    changes: int = 0
    bytes_changed: int = 0
    last_change_type: str = ""
    last_changed_at: float = 0.0

    @streaming.event
    async def observe(self, event: RecentChange):
        self.changes += 1
        self.bytes_changed += event.length_delta
        self.last_change_type = event.change_type
        self.last_changed_at = event.occurred_at
        return {
            "page_key": event.page_key,
            "wiki": event.wiki,
            "title": event.title,
            "changes": self.changes,
            "bytes_changed": self.bytes_changed,
            "last_change_type": self.last_change_type,
            "last_changed_at": self.last_changed_at,
            "url": event.url,
        }


def _read_sse_event(response: BinaryIO) -> tuple[str, dict] | None:
    event_id = None
    data = []
    while True:
        raw = response.readline()
        if not raw:
            raise EOFError("Wikimedia EventStreams disconnected")
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if not data:
                continue
            payload = json.loads("\n".join(data))
            checkpoint = event_id or payload.get("meta", {}).get("id")
            return (checkpoint, payload) if checkpoint else None
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "id":
            event_id = value
        elif field == "data":
            data.append(value)


def _decode_change(checkpoint: str, payload: dict) -> tuple[RecentChange, dict]:
    wiki = str(payload.get("wiki") or payload.get("server_name") or "unknown")
    title = str(payload.get("title") or "unknown")
    lengths = payload.get("length") or {}
    event_id = str(payload.get("meta", {}).get("id") or checkpoint)
    change = RecentChange(
        event_id=event_id,
        page_key=f"{wiki}:{title}",
        wiki=wiki,
        title=title,
        change_type=str(payload.get("type") or "unknown"),
        occurred_at=float(payload.get("timestamp") or time.time()),
        length_delta=int(lengths.get("new") or 0) - int(lengths.get("old") or 0),
        bot=bool(payload.get("bot")),
        url=payload.get("meta", {}).get("uri"),
    )
    return change, {
        "value": change.__dict__,
        "event_time": change.occurred_at,
        "key": change.page_key,
        "event_id": change.event_id,
        "checkpoint": checkpoint,
    }


async def _read_batch(
    response: BinaryIO,
    max_size: int,
    max_delay: float,
) -> list[dict]:
    started = time.monotonic()
    records = []
    while len(records) < max_size:
        event = await asyncio.to_thread(_read_sse_event, response)
        if event is not None:
            records.append(_decode_change(*event)[1])
        if records and time.monotonic() - started >= max_delay:
            break
    return records


async def deploy(client: Client) -> None:
    try:
        await client.stream_info(INPUT_STREAM)
    except RuntimeError as error:
        if "not found" not in str(error):
            raise
        await client.create_stream(
            INPUT_STREAM,
            options=StreamOptions(
                watermark_mode=WatermarkMode.SOURCE_MANAGED,
                allowed_lateness=timedelta(minutes=5),
            ),
        )
    await client.deploy_process(
        WikimediaPageActivity,
        input=INPUT_STREAM,
        process_id=PROCESS_ID,
        options=ProcessOptions(
            key="page_key",
            max_concurrency=512,
            capacity=100_000,
            retry_concurrency=16,
            max_attempts=5,
            batch_size=256,
            batch_delay=0.01,
            task_queue="public-streams",
        ),
    )


def _open_feed(checkpoint: str | None) -> BinaryIO:
    headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    if checkpoint:
        headers["Last-Event-ID"] = checkpoint
    return urlopen(Request(FEED_URL, headers=headers), timeout=60)


async def run(client: Client, *, duration: float, batch_size: int, batch_delay: float) -> None:
    await deploy(client)
    started = time.monotonic()
    committed = 0
    writer = client.stream_writer(INPUT_STREAM, source_id=SOURCE_ID)
    async with writer:
        while duration <= 0 or time.monotonic() - started < duration:
            response = None
            try:
                response = await asyncio.to_thread(_open_feed, writer.checkpoint)
                while duration <= 0 or time.monotonic() - started < duration:
                    records = await _read_batch(response, batch_size, batch_delay)
                    await writer.publish_many(records)
                    committed += len(records)
                    watermark = max(record["event_time"] for record in records) - 30
                    await writer.advance_watermark(watermark)
                    elapsed = max(time.monotonic() - started, 0.001)
                    logging.info(
                        "committed=%d rate=%.1f events/s checkpoint=%s",
                        committed,
                        committed / elapsed,
                        writer.checkpoint,
                    )
            except (EOFError, OSError, TimeoutError) as error:
                logging.warning("public feed disconnected: %s", error)
                await asyncio.sleep(1)
            finally:
                if response is not None:
                    response.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--batch-delay", type=float, default=0.25)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1_000:
        parser.error("--batch-size must be between 1 and 1000")
    if args.duration < 0 or args.batch_delay <= 0:
        parser.error("--duration must be non-negative and --batch-delay must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    await run(
        Client(args.target, api_key=os.environ.get("HIGHWATER_API_KEY")),
        duration=args.duration,
        batch_size=args.batch_size,
        batch_delay=args.batch_delay,
    )


if __name__ == "__main__":
    asyncio.run(main())
