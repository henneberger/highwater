from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
import os
import time
import traceback
import uuid
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from . import activity_context
from .errors import NonRetryableError
from .model import Event
from .registry import Registry
from .workflow_runner import WorkflowRunner


class _RemoteActivityClient:
    def __init__(self, target: str, task_token: str) -> None:
        self.target = target
        self.task_token = task_token

    def heartbeat(self, details: Any) -> bool:
        request = Request(
            f"{self.target}/internal/v1/activity-tasks/{self.task_token}/heartbeat",
            data=json.dumps({"details": details}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=15) as response:
                return bool(json.loads(response.read())["accepted"])
        except HTTPError:
            return False

class RustWorker:
    def __init__(
        self,
        registry: Registry,
        *,
        target: str = "http://127.0.0.1:7233",
        task_queue: str | None = None,
        worker_id: str | None = None,
        poll_interval: float = 0.005,
        lease_seconds: float = 30.0,
        process_poll_width: int | None = None,
        process_shard_offset: int = 0,
        process_partitions: tuple[int, ...] | None = None,
        process_only: bool = False,
        execution_token: str | None = None,
    ) -> None:
        self.registry = registry
        self.runner = WorkflowRunner(registry)
        self.target = target.rstrip("/")
        self.task_queue = task_queue
        self.worker_id = worker_id or str(uuid.uuid4())
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        if process_poll_width is not None and process_poll_width <= 0:
            raise ValueError("process_poll_width must be positive")
        if process_shard_offset < 0:
            raise ValueError("process_shard_offset must be non-negative")
        if process_partitions is not None and (
            not process_partitions
            or any(partition <= 0 for partition in process_partitions)
            or len(set(process_partitions)) != len(process_partitions)
        ):
            raise ValueError("process_partitions must contain unique positive integers")
        self._process_shard_cursor = process_shard_offset
        self._process_partitions = process_partitions
        self._process_only = process_only
        self._execution_token = execution_token
        default_process_poll_width = (
            8
            if any(
                getattr(definition, "__temporal_code_process_batch_run__", None) is not None
                for definition in registry.workflows.values()
            )
            else 1
        )
        self._process_poll_width = process_poll_width or default_process_poll_width

    def _request(self, path: str, body: Any) -> Any:
        request = Request(
            f"{self.target}{path}",
            data=json.dumps(body, separators=(",", ":")).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=35) as response:
                if response.status == 204:
                    return None
                return json.loads(response.read() or b"{}")
        except HTTPError as error:
            message = json.loads(error.read() or b"{}").get("error", str(error))
            raise RuntimeError(message) from error

    def _poll_body(self) -> dict[str, Any]:
        return {
            "protocol_version": 1,
            "worker_id": self.worker_id,
            "execution_token": self._execution_token,
            "task_queue": self.task_queue,
            "build_ids": self.registry.build_ids(),
            "lease_seconds": self.lease_seconds,
        }

    @staticmethod
    def _events(values: list[dict[str, Any]]) -> list[Event]:
        return [
            Event(event["id"], event["workflow_id"], event["type"], event["data"], event["created_at"])
            for event in values
        ]

    async def _workflow_once(self) -> bool:
        activations = await asyncio.to_thread(
            self._request, "/internal/v1/workflow-tasks/poll-batch", self._poll_body(),
        )
        if not activations:
            return False

        async def execute(activation: dict[str, Any]) -> dict[str, Any]:
            events = self._events(activation["history"])
            completion: dict[str, Any] = {
                "protocol_version": 1,
                "task_token": activation["task_token"],
                "history_event_id": events[-1].id,
                "commands": [],
            }
            try:
                result = await self.runner.activate(
                    activation["workflow_id"], activation["workflow_type"], events,
                )
                completion["history_event_id"] = result.history_event_id
                completion["commands"] = [
                    {"type": command.type, "attributes": command.attributes}
                    for command in result.commands
                ]
            except Exception as error:
                completion["failure"] = "".join(
                    traceback.format_exception_only(type(error), error)
                ).strip()
            return completion

        workflow_type = activations[0]["workflow_type"]
        definition = self.registry.workflows[workflow_type]
        batch_run = getattr(definition, "__temporal_code_process_batch_run__", None)
        if batch_run is None:
            completions = await asyncio.gather(
                *(execute(activation) for activation in activations)
            )
        else:
            event_lists = [self._events(activation["history"]) for activation in activations]
            try:
                results = await batch_run(
                    definition(),
                    [events[0].data["args"][0] for events in event_lists],
                )
            except Exception as error:
                failure = "".join(
                    traceback.format_exception_only(type(error), error)
                ).strip()
                completions = [
                    {
                        "protocol_version": 1,
                        "task_token": activation["task_token"],
                        "history_event_id": events[-1].id,
                        "commands": [],
                        "failure": failure,
                    }
                    for activation, events in zip(activations, event_lists, strict=True)
                ]
            else:
                completions = [
                    {
                        "protocol_version": 1,
                        "task_token": activation["task_token"],
                        "history_event_id": events[-1].id,
                        "commands": [{"type": "COMPLETE_WORKFLOW", "attributes": {"result": result}}],
                    }
                    for activation, events, result in zip(
                        activations, event_lists, results, strict=True
                    )
                ]
        try:
            await asyncio.to_thread(
                self._request, "/internal/v1/workflow-tasks/complete-batch", completions,
            )
        except RuntimeError as error:
            if "task lease lost" not in str(error):
                raise
        return True

    async def _process_once(self) -> bool:
        bodies = []
        if self._process_partitions is not None:
            for partition_id in self._process_partitions:
                body = self._poll_body()
                body["partition_id"] = partition_id
                bodies.append(body)
        else:
            for _ in range(self._process_poll_width):
                body = self._poll_body()
                body["shard_cursor"] = self._process_shard_cursor
                self._process_shard_cursor += 1
                bodies.append(body)
        polled = await asyncio.gather(*(
            asyncio.to_thread(
                self._request, "/internal/v1/process-tasks/poll-batch", body,
            )
            for body in bodies
        ))
        batches = [batch for batch in polled if batch is not None]
        if not batches:
            return False
        renewal_stops = [asyncio.Event() for _ in batches]
        renewal_tasks = [
            asyncio.create_task(self._renew_process_batch(batch, stop))
            for batch, stop in zip(batches, renewal_stops, strict=True)
        ]
        completions = []
        for batch in batches:
            definition = self.registry.workflows[batch["workflow_type"]]
            max_size = getattr(definition, "__temporal_code_batch_max_size__", 64)
            items = []
            envelopes = batch["envelopes"]
            for offset in range(0, len(envelopes), max_size):
                chunk = envelopes[offset:offset + max_size]
                batch_run = getattr(definition, "__temporal_code_process_batch_run__", None)
                try:
                    if batch_run is not None:
                        results = await batch_run(definition(), chunk)
                    else:
                        run = getattr(definition, "__temporal_code_process_run__")
                        results = await asyncio.gather(*(
                            run(definition(), envelope) for envelope in chunk
                        ))
                except Exception as error:
                    failure = "".join(
                        traceback.format_exception_only(type(error), error)
                    ).strip()
                    items.extend({"failure": failure} for _ in chunk)
                else:
                    items.extend({"result": result} for result in results)
            completions.append({
                "protocol_version": 1,
                "lease_token": batch["lease_token"],
                "partition_id": batch["partition_id"],
                "owner_epoch": batch["owner_epoch"],
                "activation_sequence": batch["activation_sequence"],
                "items": items,
            })

        async def complete(body: dict[str, Any]) -> None:
            try:
                await asyncio.to_thread(
                    self._request, "/internal/v1/process-tasks/complete-batch", body,
                )
            except RuntimeError as error:
                if "task lease lost" not in str(error):
                    raise

        try:
            await asyncio.gather(*(complete(body) for body in completions))
        finally:
            for stop in renewal_stops:
                stop.set()
            await asyncio.gather(*renewal_tasks, return_exceptions=True)
        return True

    async def _renew_process_batch(
        self,
        batch: dict[str, Any],
        stop: asyncio.Event,
    ) -> None:
        lease_expires = float(batch["lease_expires"])
        while not stop.is_set():
            delay = max(0.25, min(self.lease_seconds / 2, (lease_expires - time.time()) / 2))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                return
            except TimeoutError:
                pass
            try:
                response = await asyncio.to_thread(
                    self._request,
                    "/internal/v1/process-tasks/renew",
                    {
                        "protocol_version": 1,
                        "lease_token": batch["lease_token"],
                        "partition_id": batch["partition_id"],
                        "owner_epoch": batch["owner_epoch"],
                        "activation_sequence": batch["activation_sequence"],
                        "extend_seconds": self.lease_seconds,
                    },
                )
                lease_expires = float(response["lease_expires"])
            except RuntimeError as error:
                if "task lease lost" in str(error):
                    return
                await asyncio.sleep(0.25)
            except OSError:
                await asyncio.sleep(0.25)

    async def _query_once(self) -> bool:
        task = await asyncio.to_thread(
            self._request, "/internal/v1/query-tasks/poll", self._poll_body(),
        )
        if task is None:
            return False
        completion: dict[str, Any] = {
            "protocol_version": 1,
            "task_token": task["task_token"],
        }
        try:
            completion["result"] = await self.runner.query(
                task["workflow_id"], task["workflow_type"], self._events(task["history"]),
                task["name"], task["args"],
            )
        except Exception as error:
            completion["error"] = "".join(
                traceback.format_exception_only(type(error), error)
            ).strip()
        await asyncio.to_thread(
            self._request, "/internal/v1/query-tasks/complete", completion,
        )
        return True

    async def _activity_once(self) -> bool:
        task = await asyncio.to_thread(
            self._request, "/internal/v1/activity-tasks/poll", self._poll_body(),
        )
        if task is None:
            return False
        completion: dict[str, Any] = {
            "protocol_version": 1,
            "task_token": task["task_token"],
            "non_retryable": False,
        }
        function = self.registry.activities.get(task["name"])
        if function is None:
            completion.update(error=f"activity {task['name']!r} is not registered", non_retryable=True)
        else:
            remote_activity = _RemoteActivityClient(self.target, task["task_token"])
            context = activity_context.ActivityContext(
                task["id"], task["workflow_id"], task["attempt"],
                remote_activity.heartbeat,
                lambda: not remote_activity.heartbeat(None),
            )
            token = activity_context._context.set(context)
            try:
                async def invoke() -> Any:
                    if inspect.iscoroutinefunction(function):
                        return await function(*task["args"])
                    return await asyncio.to_thread(function, *task["args"])

                timeout = task.get("start_to_close_timeout")
                result = await asyncio.wait_for(invoke(), timeout) if timeout is not None else await invoke()
            except TimeoutError:
                completion["error"] = "start-to-close timeout"
            except NonRetryableError as error:
                completion.update(error=repr(error), non_retryable=True)
            except Exception as error:
                completion["error"] = repr(error)
            else:
                completion["result"] = result
            finally:
                activity_context._context.reset(token)
        try:
            await asyncio.to_thread(
                self._request, "/internal/v1/activity-tasks/complete", completion,
            )
        except RuntimeError as error:
            if "task lease lost" not in str(error):
                raise
        return True

    async def run_forever(self) -> None:
        while True:
            if self._process_only:
                if not await self._process_once():
                    await asyncio.sleep(self.poll_interval)
                continue
            process_work, workflow_work, activity_work, query_work = await asyncio.gather(
                self._process_once(), self._workflow_once(), self._activity_once(), self._query_once(),
            )
            if not process_work and not workflow_work and not activity_work and not query_work:
                await asyncio.sleep(self.poll_interval)


def main() -> None:
    parser = argparse.ArgumentParser(prog="temporal-code-worker")
    parser.add_argument("module", help="Python module containing annotated workflows and activities")
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    parser.add_argument("--task-queue")
    parser.add_argument("--process-poll-width", type=int)
    parser.add_argument("--process-shard-offset", type=int, default=0)
    parser.add_argument(
        "--process-partitions",
        help="comma-separated process partition ids assigned to this execution instance",
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="run only keyed Process execution",
    )
    arguments = parser.parse_args()
    registry = Registry().discover(importlib.import_module(arguments.module))
    worker = RustWorker(
        registry,
        target=arguments.target,
        task_queue=arguments.task_queue,
        process_poll_width=arguments.process_poll_width,
        process_shard_offset=arguments.process_shard_offset,
        process_partitions=(
            tuple(int(value) for value in arguments.process_partitions.split(","))
            if arguments.process_partitions
            else None
        ),
        process_only=arguments.process_only,
        execution_token=os.environ.get("HIGHWATER_EXECUTION_TOKEN"),
    )
    try:
        asyncio.run(worker.run_forever())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
