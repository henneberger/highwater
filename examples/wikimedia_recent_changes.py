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
PROCESS_ID = "wikimedia-public-print"
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
    key="wiki",
    event_time="occurred_at",
    build_id="wikimedia-print-sink-v1",
)
@dataclass
class WikimediaPrintSink:
    changes: int = 0
    last_changed_at: float = 0.0

    @streaming.event
    async def observe(self, event: RecentChange):
        self.changes += 1
        self.last_changed_at = event.occurred_at
        if self.changes % 100 == 0:
            print(json.dumps({
                "stream": INPUT_STREAM,
                "events_processed_for_key": self.changes,
                "event_id": event.event_id,
                "wiki": event.wiki,
                "title": event.title,
                "type": event.change_type,
                "event_time": event.occurred_at,
                "length_delta": event.length_delta,
                "bot": event.bot,
                "url": event.url,
            }, separators=(",", ":")), flush=True)
        return streaming.transition(state={
            "changes": self.changes,
            "last_changed_at": self.last_changed_at,
        })


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
        WikimediaPrintSink,
        input=INPUT_STREAM,
        process_id=PROCESS_ID,
        options=ProcessOptions(
            key="wiki",
            max_concurrency=512,
            capacity=100_000,
            retry_concurrency=16,
            max_attempts=5,
            discard_input_on_success=True,
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


def _checkpoint_at_watermark(checkpoint: str | None, watermark: float | None) -> str | None:
    if checkpoint is None or watermark is None:
        return checkpoint
    try:
        positions = json.loads(checkpoint)
    except (TypeError, json.JSONDecodeError):
        return checkpoint
    floor = int(watermark * 1_000)
    changed = False
    for position in positions if isinstance(positions, list) else ():
        timestamp = position.get("timestamp") if isinstance(position, dict) else None
        if isinstance(timestamp, int) and timestamp < floor:
            position["timestamp"] = floor
            changed = True
    return json.dumps(positions, separators=(",", ":")) if changed else checkpoint


async def run(
    client: Client,
    *,
    duration: float,
    batch_size: int,
    batch_delay: float,
    max_rate: float,
) -> None:
    await deploy(client)
    stream = await client.stream_info(INPUT_STREAM)
    watermark_floor = stream.watermark
    started = time.monotonic()
    committed = 0
    next_publish_at = started
    writer = client.stream_writer(INPUT_STREAM, source_id=SOURCE_ID)
    async with writer:
        while duration <= 0 or time.monotonic() - started < duration:
            response = None
            try:
                resume_checkpoint = _checkpoint_at_watermark(
                    writer.checkpoint,
                    watermark_floor,
                )
                response = await asyncio.to_thread(_open_feed, resume_checkpoint)
                while duration <= 0 or time.monotonic() - started < duration:
                    records = await _read_batch(response, batch_size, batch_delay)
                    next_publish_at = max(next_publish_at, time.monotonic())
                    await writer.publish_many(records)
                    committed += len(records)
                    next_publish_at += len(records) / max_rate
                    watermark = max(record["event_time"] for record in records) - 30
                    if watermark_floor is None or watermark > watermark_floor:
                        await writer.advance_watermark(watermark)
                        watermark_floor = watermark
                    elapsed = max(time.monotonic() - started, 0.001)
                    logging.info(
                        "committed=%d rate=%.1f events/s checkpoint=%s",
                        committed,
                        committed / elapsed,
                        writer.checkpoint,
                    )
                    await asyncio.sleep(max(0.0, next_publish_at - time.monotonic()))
            except (EOFError, OSError, TimeoutError) as error:
                logging.warning("public feed disconnected: %s", error)
                await asyncio.sleep(1)
            finally:
                if response is not None:
                    response.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-delay", type=float, default=0.25)
    parser.add_argument("--max-rate", type=float, default=40.0)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1_000:
        parser.error("--batch-size must be between 1 and 1000")
    if args.duration < 0 or args.batch_delay <= 0 or args.max_rate <= 0:
        parser.error("--duration must be non-negative; delays and rates must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    await run(
        Client(args.target, api_key=os.environ.get("HIGHWATER_API_KEY")),
        duration=args.duration,
        batch_size=args.batch_size,
        batch_delay=args.batch_delay,
        max_rate=args.max_rate,
    )


if __name__ == "__main__":
    asyncio.run(main())
