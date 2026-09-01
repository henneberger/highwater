from __future__ import annotations

import asyncio
import json
import os
import struct
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .errors import StreamBackpressure, WorkflowCancelled, WorkflowFailed
from .model import (
    ChangeKind,
    FilterSpec,
    DeduplicateOutput,
    DeduplicateSpec,
    Event,
    EventTimeGate,
    IntervalJoinOutput,
    IntervalJoinSpec,
    ProcessSpec,
    ProcessOptions,
    StreamInfo,
    StreamOptions,
    StreamRecord,
    TemporalJoinOutput,
    TemporalJoinSpec,
    WindowAggregateSpec,
    WorkflowOptions,
    WorkflowStatus,
)


def _timestamp(value: float | datetime) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("event-time datetimes must include a timezone")
        return value.timestamp()
    return float(value)


def _duration(value: float | timedelta | None) -> float | None:
    if isinstance(value, timedelta):
        return value.total_seconds()
    return value


class WorkflowHandle:
    def __init__(self, client: "Client", workflow_id: str) -> None:
        self.client = client
        self.id = workflow_id

    async def result(self, *, timeout: float | None = None) -> Any:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            info = await self.client._request("GET", f"/workflows/{quote(self.id, safe='')}")
            status = WorkflowStatus(info["status"])
            if status == WorkflowStatus.COMPLETED:
                return info["result"]
            if status == WorkflowStatus.CANCELLED:
                raise WorkflowCancelled(info["error"])
            if status in {WorkflowStatus.FAILED, WorkflowStatus.TIMED_OUT, WorkflowStatus.TERMINATED}:
                raise WorkflowFailed(info["error"])
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(self.id)
            await asyncio.sleep(self.client.poll_interval)

    async def signal(self, name: str, *args: Any) -> None:
        await self.client._request(
            "POST", f"/workflows/{quote(self.id, safe='')}/signals/{quote(name, safe='')}", {"args": args},
        )

    async def query(self, name: str, *args: Any) -> Any:
        response = await self.client._request(
            "POST", f"/workflows/{quote(self.id, safe='')}/queries/{quote(name, safe='')}", {"args": args},
        )
        return response["result"]

    async def update(self, name: str, *args: Any, timeout: float | None = None) -> Any:
        update_id = str(uuid.uuid4())
        await self.client._request(
            "POST", f"/workflows/{quote(self.id, safe='')}/updates/{quote(name, safe='')}",
            {"args": args, "update_id": update_id},
        )
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            for event in await self.history():
                if event.type == "UPDATE_COMPLETED" and event.data["update_id"] == update_id:
                    return event.data.get("result")
                if event.type == "UPDATE_FAILED" and event.data["update_id"] == update_id:
                    raise RuntimeError(event.data["error"])
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(update_id)
            await asyncio.sleep(self.client.poll_interval)

    async def cancel(self) -> None:
        await self.client._request("POST", f"/workflows/{quote(self.id, safe='')}/cancel", {})

    async def terminate(self, reason: str = "terminated") -> None:
        await self.client._request(
            "POST", f"/workflows/{quote(self.id, safe='')}/terminate", {"reason": reason},
        )

    async def history(self) -> list[Event]:
        values = await self.client._request("GET", f"/workflows/{quote(self.id, safe='')}/history")
        return [Event(**value) for value in values]


def _field_path(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        current = current[part] if isinstance(current, dict) else getattr(current, part)
    return current


class ProcessHandle:
    def __init__(
        self,
        client: "Client",
        process_id: str,
        input_stream: str,
        key_field: str | None,
        event_time_field: str | None = None,
        owns_input: bool = False,
        direct_ingress: bool = False,
    ) -> None:
        self.client = client
        self.id = process_id
        self.input = input_stream
        self.key_field = key_field
        self.event_time_field = event_time_field
        self.owns_input = owns_input
        self.direct_ingress = direct_ingress

    async def send(
        self,
        event: Any,
        *,
        event_time: float | datetime | None = None,
        key: str | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        selected_key = key
        if selected_key is None and self.key_field is not None:
            selected_key = str(_field_path(event, self.key_field))
        if selected_key is None or not selected_key:
            raise ValueError("a process event requires a key or configured key field")
        selected_event_time = event_time
        if selected_event_time is None and self.event_time_field is not None:
            selected_event_time = _field_path(event, self.event_time_field)
        if selected_event_time is None:
            selected_event_time = time.time()
        value = asdict(event) if is_dataclass(event) else event
        stable_event_id = event_id or str(uuid.uuid4())
        delay = 0.01
        while True:
            try:
                if self.direct_ingress:
                    responses = await self.client._request(
                        "POST",
                        f"/processes/{quote(self.id, safe='')}/events",
                        {"records": [{
                            "partition": 0,
                            "event_time": _timestamp(selected_event_time),
                            "key": selected_key,
                            "value": value,
                            "kind": ChangeKind.UPSERT,
                            "event_id": stable_event_id,
                        }]},
                    )
                    return responses[0]
                return await self.client.publish_event(
                    self.input,
                    value,
                    event_time=selected_event_time,
                    key=selected_key,
                    event_id=stable_event_id,
                )
            except StreamBackpressure:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)

    async def state(self, key: str) -> Any:
        return await self.client.process_state(self.id, key)

    async def send_many(self, events: list[Any]) -> Any:
        if not events:
            return []
        if self.direct_ingress:
            payload = bytearray(b"TCP1")
            payload.extend(struct.pack("<I", len(events)))
            batch_time = time.time()
            for event in events:
                key = (
                    str(_field_path(event, self.key_field))
                    if self.key_field is not None
                    else None
                )
                if not key:
                    raise ValueError("batched process events require a configured key field")
                event_time = (
                    _field_path(event, self.event_time_field)
                    if self.event_time_field is not None
                    else batch_time
                )
                value = asdict(event) if is_dataclass(event) else event
                encoded_key = key.encode()
                encoded_value = json.dumps(value, separators=(",", ":")).encode()
                payload.extend(struct.pack(
                    "<dII16s",
                    _timestamp(event_time),
                    len(encoded_key),
                    len(encoded_value),
                    uuid.uuid4().bytes,
                ))
                payload.extend(encoded_key)
                payload.extend(encoded_value)
            delay = 0.01
            while True:
                try:
                    return await self.client._request_bytes(
                        f"/processes/{quote(self.id, safe='')}/events",
                        bytes(payload),
                    )
                except StreamBackpressure:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 1.0)
        records = []
        for event in events:
            key = str(_field_path(event, self.key_field)) if self.key_field is not None else None
            if not key:
                raise ValueError("batched process events require a configured key field")
            event_time = (
                _field_path(event, self.event_time_field)
                if self.event_time_field is not None
                else time.time()
            )
            records.append({
                "partition": 0,
                "event_time": _timestamp(event_time),
                "key": key,
                "value": asdict(event) if is_dataclass(event) else event,
                "kind": ChangeKind.UPSERT,
                "event_id": str(uuid.uuid4()),
            })
        delay = 0.01
        while True:
            try:
                return await self.client.publish_events(self.input, records)
            except StreamBackpressure:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)

    async def info(self) -> dict[str, Any]:
        return await self.client.process(self.id)

    async def complete_through(self, event_time: float | datetime) -> None:
        await self.client._request(
            "POST",
            f"/processes/{quote(self.id, safe='')}/complete-through",
            {"event_time": _timestamp(event_time)},
        )

    async def drain(self, *, timeout: float | None = None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            current = await self.client.process(self.id)
            if current["pending"] == 0 and current["running"] == 0:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(self.id)
            await asyncio.sleep(self.client.poll_interval)

    async def finish(self, *, timeout: float | None = None) -> None:
        if not self.owns_input:
            raise RuntimeError("finish is only available for a process-owned input stream")
        stream = await self.client.stream_info(self.input)
        for partition in stream.partitions:
            if not partition["sealed"]:
                await self.client.seal_partition(self.input, partition["partition"])
        await self.drain(timeout=timeout)


class StreamWriter:
    def __init__(
        self,
        client: "Client",
        stream: str,
        source_id: str,
        partition: int,
        bounded: bool,
    ) -> None:
        if not source_id.strip():
            raise ValueError("source_id must not be empty")
        self.client = client
        self.stream = stream
        self.source_id = source_id
        self.partition = partition
        self.bounded = bounded
        self._next_offset: int | None = None
        self._source_epoch: int | None = None
        self._claim_deadline = 0.0

    async def __aenter__(self) -> "StreamWriter":
        await self._load_cursor()
        await self._claim()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if exc is None and self.bounded:
            await self.seal()

    async def _load_cursor(self) -> None:
        if self._next_offset is None:
            cursor = await self.client.source_cursor(
                self.stream, self.source_id, partition=self.partition,
            )
            self._next_offset = cursor["next_offset"]

    @property
    def next_offset(self) -> int:
        if self._next_offset is None:
            raise RuntimeError("stream writer has not been opened")
        return self._next_offset

    async def _claim(self) -> None:
        lease = await self.client._request(
            "POST",
            f"/streams/{quote(self.stream, safe='')}/partitions/{self.partition}/"
            f"sources/{quote(self.source_id, safe='')}/claim",
            {"lease_seconds": 30.0},
        )
        self._source_epoch = lease["epoch"]
        self._claim_deadline = time.monotonic() + 15.0

    async def publish(
        self,
        value: Any,
        *,
        event_time: float | datetime,
        key: str | None = None,
        kind: ChangeKind = ChangeKind.UPSERT,
    ) -> dict[str, Any]:
        await self._load_cursor()
        if self._source_epoch is None or time.monotonic() >= self._claim_deadline:
            await self._claim()
        offset = self._next_offset
        delay = 0.01
        while True:
            try:
                response = await self.client.publish_event(
                    self.stream,
                    value,
                    event_time=event_time,
                    partition=self.partition,
                    key=key,
                    kind=kind,
                    source_id=self.source_id,
                    source_partition=self.partition,
                    source_offset=offset,
                    source_epoch=self._source_epoch,
                )
                break
            except StreamBackpressure:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)
                if time.monotonic() >= self._claim_deadline:
                    await self._claim()
        self._next_offset = offset + 1
        return response

    async def advance_watermark(self, event_time: float | datetime) -> StreamInfo:
        return await self.client.advance_watermark(self.stream, self.partition, event_time)

    async def seal(self) -> StreamInfo:
        return await self.client.seal_partition(self.stream, self.partition)


class Client:
    def __init__(
        self,
        target: str = "http://127.0.0.1:7233",
        *,
        poll_interval: float = 0.05,
        api_key: str | None = None,
    ) -> None:
        self.target = target.rstrip("/")
        self.poll_interval = poll_interval
        self.api_key = api_key or os.environ.get("HIGHWATER_API_KEY")

    def _headers(self, content_type: str) -> dict[str, str]:
        headers = {"Content-Type": content_type}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _request(self, method: str, path: str, body: Any = None) -> Any:
        def send() -> Any:
            data = None if body is None else json.dumps(body).encode()
            request = Request(
                f"{self.target}{path}", data=data, method=method,
                headers=self._headers("application/json"),
            )
            try:
                with urlopen(request, timeout=15) as response:
                    return json.loads(response.read() or b"null")
            except HTTPError as error:
                payload = json.loads(error.read() or b"{}")
                if error.code == 429:
                    raise StreamBackpressure(payload.get("error", str(error))) from error
                raise RuntimeError(payload.get("error", str(error))) from error

        return await asyncio.to_thread(send)

    async def _request_bytes(self, path: str, body: bytes) -> Any:
        def send() -> Any:
            request = Request(
                f"{self.target}{path}/packed",
                data=body,
                method="POST",
                headers=self._headers("application/x-highwater-batch"),
            )
            try:
                with urlopen(request, timeout=30) as response:
                    return json.loads(response.read() or b"null")
            except HTTPError as error:
                payload = json.loads(error.read() or b"{}")
                if error.code == 429:
                    raise StreamBackpressure(payload.get("error", str(error))) from error
                raise RuntimeError(payload.get("error", str(error))) from error

        return await asyncio.to_thread(send)

    async def start_workflow(
        self,
        workflow: str | type,
        *args: Any,
        workflow_id: str | None = None,
        options: WorkflowOptions | None = None,
    ) -> WorkflowHandle:
        workflow_type = workflow if isinstance(workflow, str) else getattr(workflow, "__highwater_workflow__", workflow.__name__)
        response = await self._request("POST", "/workflows", {
            "workflow_type": workflow_type,
            "args": args,
            "workflow_id": workflow_id,
            "options": asdict(options or WorkflowOptions()),
        })
        return WorkflowHandle(self, response["workflow_id"])

    def get_workflow_handle(self, workflow_id: str) -> WorkflowHandle:
        return WorkflowHandle(self, workflow_id)

    async def create_stream(self, name: str, *, options: StreamOptions | None = None) -> StreamInfo:
        selected = options or StreamOptions()
        body = {"name": name, **asdict(selected)}
        for field in (
            "max_out_of_orderness",
            "idle_timeout",
            "allowed_lateness",
            "alignment_max_drift",
        ):
            body[field] = _duration(body[field])
        value = await self._request("POST", "/streams", body)
        return await self.stream_info(value["name"])

    async def stream_info(self, name: str) -> StreamInfo:
        value = await self._request("GET", f"/streams/{quote(name, safe='')}")
        return StreamInfo(**value)

    async def source_cursor(
        self, stream: str, source_id: str, *, partition: int = 0,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/streams/{quote(stream, safe='')}/sources/"
            f"{quote(source_id, safe='')}/partitions/{partition}/cursor",
        )

    async def publish_event(
        self,
        stream: str,
        value: Any,
        *,
        event_time: float | datetime,
        partition: int = 0,
        key: str | None = None,
        kind: ChangeKind = ChangeKind.UPSERT,
        event_id: str | None = None,
        source_id: str | None = None,
        source_partition: int | None = None,
        source_offset: int | None = None,
        source_epoch: int | None = None,
    ) -> dict[str, Any]:
        return await self._request("POST", f"/streams/{quote(stream, safe='')}/records", {
            "partition": partition,
            "event_time": _timestamp(event_time),
            "key": key,
            "value": value,
            "kind": kind,
            "event_id": event_id,
            "source_id": source_id,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "source_epoch": source_epoch,
        })

    async def publish_events(
        self,
        stream: str,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not 1 <= len(records) <= 1_000:
            raise ValueError("record batches must contain between 1 and 1000 events")
        return await self._request(
            "POST",
            f"/streams/{quote(stream, safe='')}/records/batch",
            {"records": records},
        )

    async def advance_watermark(
        self, stream: str, partition: int, event_time: float | datetime,
    ) -> StreamInfo:
        await self._request(
            "POST",
            f"/streams/{quote(stream, safe='')}/partitions/{partition}/watermark",
            {"event_time": _timestamp(event_time)},
        )
        return await self.stream_info(stream)

    def stream_writer(
        self,
        stream: str,
        *,
        source_id: str,
        partition: int = 0,
        bounded: bool = False,
    ) -> StreamWriter:
        return StreamWriter(self, stream, source_id, partition, bounded)

    async def seal_partition(self, stream: str, partition: int) -> StreamInfo:
        await self._request(
            "POST", f"/streams/{quote(stream, safe='')}/partitions/{partition}/seal", {},
        )
        return await self.stream_info(stream)

    async def read_stream(self, stream: str) -> list[StreamRecord]:
        values = await self._request("GET", f"/streams/{quote(stream, safe='')}/records")
        return [StreamRecord(**value) for value in values]

    async def read_late_stream(self, stream: str) -> list[StreamRecord]:
        values = await self._request("GET", f"/streams/{quote(stream, safe='')}/late-records")
        return [StreamRecord(**value) for value in values]

    async def window_schedule(self, schedule_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/stream-schedules/{quote(schedule_id, safe='')}",
        )

    async def deploy(
        self,
        spec: DeduplicateSpec | FilterSpec | IntervalJoinSpec | ProcessSpec | TemporalJoinSpec | WindowAggregateSpec,
    ) -> dict[str, Any]:
        if isinstance(spec, ProcessSpec):
            workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
                spec.workflow,
                "__highwater_workflow__",
                spec.workflow.__name__,
            )
            return await self._request("POST", "/processes", {
                "process_id": spec.process_id,
                "stream": spec.input,
                "workflow_type": workflow_type,
                "key_field": spec.key,
                "event_time_field": spec.event_time,
                "state_version": spec.state_version,
                "build_id": spec.build_id,
                "migrations_from": spec.migrations_from,
                "task_queue": spec.task_queue,
                "event_time_gate": spec.event_time_gate,
                "max_concurrent_keys": spec.max_concurrency,
                "mailbox_capacity": spec.capacity,
                "batch_max_size": spec.batch_size,
                "batch_max_delay": spec.batch_delay,
            })
        if isinstance(spec, WindowAggregateSpec):
            workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
                spec.workflow,
                "__highwater_workflow__",
                spec.workflow.__name__,
            )
            return await self._request("POST", "/stream-schedules", {
                "schedule_id": spec.operator_id,
                "stream": spec.stream,
                "workflow_type": workflow_type,
                "window_size": spec.window_size,
                "slide": spec.slide,
                "start_at": spec.start_at,
                "task_queue": spec.task_queue,
                "emit_empty_windows": spec.emit_empty_windows,
                "aggregation": spec.aggregation,
                "value_field": spec.value_field,
            })
        if isinstance(spec, FilterSpec):
            workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
                spec.workflow,
                "__highwater_workflow__",
                spec.workflow.__name__,
            )
            return await self._request("POST", "/stream-filters", {
                "operator_id": spec.operator_id,
                "stream": spec.stream,
                "workflow_type": workflow_type,
                "field": spec.field,
                "comparison": spec.comparison,
                "operand": spec.operand,
                "task_queue": spec.task_queue,
            })
        if isinstance(spec, IntervalJoinSpec):
            workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
                spec.workflow,
                "__highwater_workflow__",
                spec.workflow.__name__,
            )
            return await self._request("POST", "/interval-joins", {
                "join_id": spec.operator_id,
                "left_stream": spec.left_stream,
                "right_stream": spec.right_stream,
                "workflow_type": workflow_type,
                "lower_bound": spec.lower_bound,
                "upper_bound": spec.upper_bound,
                "task_queue": spec.task_queue,
                "join_type": spec.join_type,
            })
        if isinstance(spec, DeduplicateSpec):
            workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
                spec.workflow,
                "__highwater_workflow__",
                spec.workflow.__name__,
            )
            return await self._request("POST", "/deduplicates", {
                "operator_id": spec.operator_id,
                "stream": spec.stream,
                "workflow_type": workflow_type,
                "task_queue": spec.task_queue,
            })
        if not isinstance(spec, TemporalJoinSpec):
            raise TypeError(f"unsupported operator spec: {type(spec).__name__}")
        workflow_type = spec.workflow if isinstance(spec.workflow, str) else getattr(
            spec.workflow,
            "__highwater_workflow__",
            spec.workflow.__name__,
        )
        return await self._request("POST", "/temporal-joins", {
            "join_id": spec.operator_id,
            "probe_stream": spec.probe_stream,
            "version_stream": spec.version_stream,
            "workflow_type": workflow_type,
            "task_queue": spec.task_queue,
            "join_type": spec.join_type,
        })

    async def deploy_process(
        self,
        definition: type[Any],
        *,
        input: str,
        process_id: str | None = None,
        options: ProcessOptions | None = None,
    ) -> ProcessHandle:
        name = getattr(definition, "__highwater_process__", None)
        if name is None:
            raise TypeError(f"{definition.__name__} is missing @streaming.process")
        selected = options or ProcessOptions(
            key=getattr(definition, "__highwater_process_key__"),
            event_time_gate=getattr(definition, "__highwater_process_gate__"),
        )
        identifier = process_id or name
        batch_size = selected.batch_size or getattr(
            definition, "__highwater_batch_max_size__"
        )
        batch_delay = (
            selected.batch_delay
            if selected.batch_delay is not None
            else getattr(definition, "__highwater_batch_max_delay__")
        )
        await self.deploy(ProcessSpec(
            process_id=identifier,
            input=input,
            workflow=definition,
            build_id=getattr(definition, "__highwater_build_id__"),
            state_version=getattr(definition, "__highwater_state_version__"),
            migrations_from=getattr(definition, "__highwater_migrations_from__"),
            key=selected.key,
            event_time=getattr(definition, "__highwater_process_event_time__"),
            event_time_gate=selected.event_time_gate,
            max_concurrency=selected.max_concurrency,
            capacity=selected.capacity,
            batch_size=batch_size,
            batch_delay=batch_delay,
            task_queue=selected.task_queue,
        ))
        return ProcessHandle(
            self,
            identifier,
            input,
            selected.key,
            getattr(definition, "__highwater_process_event_time__"),
        )

    async def start(
        self,
        definition: type[Any],
        *,
        source: str | None = None,
        process_id: str | None = None,
        options: ProcessOptions | None = None,
    ) -> ProcessHandle:
        if source is not None and "://" in source:
            raise NotImplementedError("external stream connectors are not implemented")
        name = getattr(definition, "__highwater_process__", None)
        if name is None:
            raise TypeError(f"{definition.__name__} is missing @streaming.process")
        identifier = process_id or name
        input_stream = source or f"{identifier}-input"
        owns_input = source is None
        if owns_input:
            try:
                await self.stream_info(input_stream)
            except RuntimeError as error:
                if "not found" not in str(error):
                    raise
                await self.create_stream(input_stream)
        handle = await self.deploy_process(
            definition,
            input=input_stream,
            process_id=identifier,
            options=options,
        )
        handle.owns_input = owns_input
        handle.direct_ingress = (
            owns_input
            and (options or ProcessOptions(
                key=getattr(definition, "__highwater_process_key__"),
                event_time_gate=getattr(definition, "__highwater_process_gate__"),
            )).event_time_gate == EventTimeGate.IMMEDIATE
        )
        return handle

    async def get_process_handle(self, process_id: str) -> ProcessHandle:
        current = await self.process(process_id)
        return ProcessHandle(
            self,
            process_id,
            current["stream"],
            current.get("key_field"),
            current.get("event_time_field"),
            direct_ingress=current.get("event_time_gate") == EventTimeGate.IMMEDIATE,
        )

    async def temporal_join(self, join_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/temporal-joins/{quote(join_id, safe='')}",
        )

    async def process(self, process_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/processes/{quote(process_id, safe='')}",
        )

    async def process_state(self, process_id: str, key: str) -> Any:
        response = await self._request(
            "GET",
            f"/processes/{quote(process_id, safe='')}/keys/{quote(key, safe='')}",
        )
        return response["state"]

    async def stream_filter(self, operator_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/stream-filters/{quote(operator_id, safe='')}",
        )

    async def read_stream_filter(self, operator_id: str) -> list[dict[str, Any]]:
        return await self._request(
            "GET", f"/stream-filters/{quote(operator_id, safe='')}/outputs",
        )

    async def read_operator_changes(self, operator_id: str) -> list[dict[str, Any]]:
        return await self._request(
            "GET", f"/operators/{quote(operator_id, safe='')}/changes",
        )

    async def connect_operator(self, operator_id: str, output_stream: str) -> dict[str, Any]:
        return await self._request("POST", "/operator-edges", {
            "operator_id": operator_id,
            "output_stream": output_stream,
        })

    async def operator_edge(self, operator_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/operator-edges/{quote(operator_id, safe='')}",
        )

    async def start_checkpoint_barrier(self) -> dict[str, Any]:
        return await self._request("POST", "/admin/checkpoint-barriers", {})

    async def checkpoint_barrier(self, checkpoint_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/admin/checkpoint-barriers/{quote(checkpoint_id, safe='')}",
        )

    async def pending_checkpoint_barriers(self, node_id: str) -> list[dict[str, Any]]:
        return await self._request(
            "GET", f"/admin/nodes/{quote(node_id, safe='')}/checkpoint-barriers",
        )

    async def acknowledge_checkpoint_barrier(
        self,
        checkpoint_id: str,
        node_id: str,
        *,
        state_handle: str,
        key_group_epochs: dict[int, int],
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/admin/checkpoint-barriers/{quote(checkpoint_id, safe='')}/acks/"
            f"{quote(node_id, safe='')}",
            {"state_handle": state_handle, "key_group_epochs": key_group_epochs},
        )

    async def read_temporal_join(self, join_id: str) -> list[TemporalJoinOutput]:
        values = await self._request(
            "GET", f"/temporal-joins/{quote(join_id, safe='')}/outputs",
        )
        return [TemporalJoinOutput(
            join_id=value["join_id"],
            probe=StreamRecord(**value["probe"]),
            version=(
                StreamRecord(**value["version"])
                if value["version"] is not None
                else None
            ),
            as_of=value["as_of"],
            watermark=value["watermark"],
            workflow_id=value["workflow_id"],
        ) for value in values]

    async def interval_join(self, join_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/interval-joins/{quote(join_id, safe='')}",
        )

    async def read_interval_join(self, join_id: str) -> list[IntervalJoinOutput]:
        values = await self._request(
            "GET", f"/interval-joins/{quote(join_id, safe='')}/outputs",
        )
        return [IntervalJoinOutput(
            join_id=value["join_id"],
            left=StreamRecord(**value["left"]),
            right=StreamRecord(**value["right"]),
            workflow_id=value["workflow_id"],
        ) for value in values]

    async def deduplicate(self, operator_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"/deduplicates/{quote(operator_id, safe='')}",
        )

    async def read_deduplicate(self, operator_id: str) -> list[DeduplicateOutput]:
        values = await self._request(
            "GET", f"/deduplicates/{quote(operator_id, safe='')}/outputs",
        )
        return [DeduplicateOutput(
            operator_id=value["operator_id"],
            record=StreamRecord(**value["record"]),
            canonical=value["canonical"],
            canonical_record=StreamRecord(**value["canonical_record"]),
            workflow_id=value["workflow_id"],
        ) for value in values]

    async def create_checkpoint(self) -> dict[str, Any]:
        return await self._request("POST", "/admin/checkpoints", {})

    async def checkpoint_manifest(self) -> dict[str, Any]:
        return await self._request("GET", "/admin/checkpoints/current")

    async def key_groups(self) -> list[dict[str, Any]]:
        return await self._request("GET", "/admin/key-groups")

    async def assign_key_group(
        self, key_group: int, owner: str, *, expected_epoch: int,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/admin/key-groups/{key_group}/assign",
            {"owner": owner, "expected_epoch": expected_epoch},
        )

    async def poll_sink(
        self, sink: str, consumer_id: str, *, lease_seconds: float = 30.0,
    ) -> dict[str, Any] | None:
        return await self._request(
            "POST",
            f"/sinks/{quote(sink, safe='')}/poll",
            {"consumer_id": consumer_id, "lease_seconds": lease_seconds},
        )

    async def ack_sink(
        self, sink: str, message_id: str, consumer_id: str,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/sinks/{quote(sink, safe='')}/messages/{quote(message_id, safe='')}/ack",
            {"consumer_id": consumer_id},
        )


RemoteClient = Client
RemoteWorkflowHandle = WorkflowHandle
