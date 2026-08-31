from __future__ import annotations

import contextvars
import inspect
import math
from collections.abc import Callable, Generator
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, TypeVar

from .errors import ActivityError, ChildWorkflowError, NonDeterminismError, WorkflowCancelled
from .model import ActivityOptions, ChildWorkflowOptions, Command, Event, RetryPolicy

T = TypeVar("T")
_runtime: contextvars.ContextVar["ReplayRuntime"] = contextvars.ContextVar("temporal_code_runtime")


def _name(value: str | Callable[..., Any]) -> str:
    return value if isinstance(value, str) else getattr(value, "__temporal_code_name__", value.__name__)


class WorkflowAwaitable(Generic[T]):
    def __init__(self, command: Command) -> None:
        self.command = command

    def __await__(self) -> Generator[Command, Any, T]:
        result = yield self.command
        return result


class ReplayRuntime:
    def __init__(self, engine: Any, workflow_id: str, instance: Any, events: list[Event]) -> None:
        self.engine = engine
        self.workflow_id = workflow_id
        self.instance = instance
        self.events = events
        self.run_number = int(events[0].data.get("run_number", 1))
        self.index = 1
        self.command_id = 0
        self.pending: list[Command] = []
        self.cancelled = False

    def bind(self):
        return _runtime.set(self)

    def unbind(self, token: contextvars.Token) -> None:
        _runtime.reset(token)

    def _dispatch_external(self) -> None:
        while self.index < len(self.events):
            event = self.events[self.index]
            if event.type == "SIGNAL_RECEIVED":
                self.engine.invoke_handler(self.instance, "signal", event.data["name"], event.data["args"])
            elif event.type == "UPDATE_REQUESTED":
                try:
                    result = self.engine.invoke_handler(self.instance, "update", event.data["name"], event.data["args"])
                    if inspect.isawaitable(result):
                        raise TypeError("update handlers must be synchronous")
                except Exception as error:
                    completion = Command("FAIL_UPDATE", {"update_id": event.data["update_id"], "error": repr(error)})
                else:
                    completion = Command("COMPLETE_UPDATE", {"update_id": event.data["update_id"], "result": result})
                following = self.events[self.index + 1] if self.index + 1 < len(self.events) else None
                if following and following.type in {"UPDATE_COMPLETED", "UPDATE_FAILED"}:
                    if following.data["update_id"] != event.data["update_id"]:
                        raise NonDeterminismError("update completion does not match its request")
                    if following.type == "UPDATE_COMPLETED" and completion.type != "COMPLETE_UPDATE":
                        raise NonDeterminismError("update previously completed but now fails")
                    if following.type == "UPDATE_FAILED" and completion.type != "FAIL_UPDATE":
                        raise NonDeterminismError("update previously failed but now completes")
                    if following.type == "UPDATE_COMPLETED" and following.data.get("result") != completion.attributes.get("result"):
                        raise NonDeterminismError("update result changed during replay")
                    self.index += 1
                else:
                    self.pending.append(completion)
            elif event.type == "CANCEL_REQUESTED":
                self.cancelled = True
            elif event.type == "WORKFLOW_TASK_FAILED":
                pass
            else:
                break
            self.index += 1
        if self.cancelled:
            raise WorkflowCancelled(f"workflow {self.workflow_id} was cancelled")

    def _next_command_id(self) -> int:
        self.command_id += 1
        return self.command_id

    def _scheduled(self, event_type: str, expected: dict[str, Any]) -> Event | None:
        self._dispatch_external()
        if self.index >= len(self.events):
            return None
        event = self.events[self.index]
        if event.type != event_type or any(event.data.get(key) != value for key, value in expected.items()):
            raise NonDeterminismError(f"expected {event_type} {expected}, found {event.type} {event.data}")
        self.index += 1
        return event

    def _completion(
        self,
        success: str,
        failure: str | None,
        command_id: int,
        failure_type: type[Exception] = ActivityError,
    ) -> tuple[bool, Any]:
        self._dispatch_external()
        if self.index >= len(self.events):
            return False, None
        event = self.events[self.index]
        if event.type == success:
            if event.data.get("command_id") != command_id:
                raise NonDeterminismError(f"{success} completed the wrong command")
            self.index += 1
            return True, event.data.get("result")
        if failure and event.type == failure:
            if event.data.get("command_id") != command_id:
                raise NonDeterminismError(f"{failure} completed the wrong command")
            self.index += 1
            raise failure_type(event.data["error"])
        return False, None

    def resolve(self, command: Command) -> tuple[bool, Any]:
        if command.type == "WAIT_CONDITION":
            self._dispatch_external()
            return bool(command.attributes["predicate"]()), None
        if command.type == "SCHEDULE_ACTIVITY":
            command_id = self._next_command_id()
            attributes = {**command.attributes, "command_id": command_id}
            expected = {key: attributes[key] for key in ("command_id", "name", "args", "options")}
            if self._scheduled("ACTIVITY_SCHEDULED", expected) is None:
                self.pending.append(Command(command.type, attributes))
                return False, None
            return self._completion("ACTIVITY_COMPLETED", "ACTIVITY_FAILED", command_id)
        if command.type == "START_TIMER":
            command_id = self._next_command_id()
            attributes = {**command.attributes, "command_id": command_id}
            if self._scheduled("TIMER_STARTED", attributes) is None:
                self.pending.append(Command(command.type, attributes))
                return False, None
            ready, _ = self._completion("TIMER_FIRED", None, command_id)
            return ready, None
        if command.type == "START_WATERMARK_TIMER":
            command_id = self._next_command_id()
            attributes = {**command.attributes, "command_id": command_id}
            if self._scheduled("WATERMARK_TIMER_STARTED", attributes) is None:
                self.pending.append(Command(command.type, attributes))
                return False, None
            ready, _ = self._completion("WATERMARK_TIMER_FIRED", None, command_id)
            return ready, None
        if command.type == "START_CHILD":
            command_id = self._next_command_id()
            attributes = {**command.attributes, "command_id": command_id}
            expected = {key: attributes[key] for key in ("command_id", "name", "args", "workflow_id", "parent_close_policy")}
            if self._scheduled("CHILD_WORKFLOW_SCHEDULED", expected) is None:
                self.pending.append(Command(command.type, attributes))
                return False, None
            return self._completion(
                "CHILD_WORKFLOW_COMPLETED", "CHILD_WORKFLOW_FAILED", command_id, ChildWorkflowError,
            )
        if command.type == "RECORD_VERSION":
            attributes = command.attributes
            event = self._scheduled("VERSION_MARKER", {"change_id": attributes["change_id"]})
            if event is None:
                self.pending.append(command)
                return False, None
            if not attributes["minimum"] <= event.data["version"] <= attributes["maximum"]:
                raise NonDeterminismError(
                    f"recorded version {event.data['version']} is outside the supported range"
                )
            return True, event.data["version"]
        if command.type == "CONTINUE_AS_NEW":
            self.pending.append(command)
            return False, None
        raise RuntimeError(f"unknown workflow command {command.type}")


def current_runtime() -> ReplayRuntime:
    try:
        return _runtime.get()
    except LookupError as error:
        raise RuntimeError("workflow primitive called outside a workflow") from error


async def execute_activity(
    fn: str | Callable[..., Any],
    *args: Any,
    options: ActivityOptions | None = None,
    retry_policy: RetryPolicy | None = None,
) -> Any:
    if options is not None and retry_policy is not None:
        raise ValueError("provide options or retry_policy, not both")
    selected = options or ActivityOptions(retry_policy=retry_policy or RetryPolicy())
    attributes = {
        "name": _name(fn),
        "args": list(args),
        "options": {
            "retry_policy": {
                "maximum_attempts": selected.retry_policy.maximum_attempts,
                "initial_interval": selected.retry_policy.initial_interval,
                "backoff_coefficient": selected.retry_policy.backoff_coefficient,
                "maximum_interval": selected.retry_policy.maximum_interval,
            },
            "task_queue": selected.task_queue,
            "schedule_to_close_timeout": selected.schedule_to_close_timeout,
            "start_to_close_timeout": selected.start_to_close_timeout,
            "heartbeat_timeout": selected.heartbeat_timeout,
        },
    }
    return await WorkflowAwaitable(Command("SCHEDULE_ACTIVITY", attributes))


async def sleep(duration: float | timedelta) -> None:
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else float(duration)
    if seconds < 0:
        raise ValueError("timer duration must not be negative")
    await WorkflowAwaitable(Command("START_TIMER", {"seconds": seconds}))


async def wait_for_watermark(stream: str, event_time: float) -> None:
    if not stream:
        raise ValueError("stream must not be empty")
    if not math.isfinite(event_time):
        raise ValueError("event_time must be finite")
    await WorkflowAwaitable(Command("START_WATERMARK_TIMER", {
        "stream": stream,
        "event_time": float(event_time),
    }))


async def wait_condition(predicate: Callable[[], bool]) -> None:
    await WorkflowAwaitable(Command("WAIT_CONDITION", {"predicate": predicate}))


async def execute_child_workflow(
    fn: str | type,
    *args: Any,
    workflow_id: str | None = None,
    options: ChildWorkflowOptions | None = None,
) -> Any:
    name = fn if isinstance(fn, str) else getattr(fn, "__temporal_code_workflow__", fn.__name__)
    selected = options or ChildWorkflowOptions(workflow_id=workflow_id)
    if workflow_id is not None and options is not None:
        raise ValueError("workflow_id must be provided directly or through options")
    runtime = current_runtime()
    child_id = selected.workflow_id or f"{runtime.workflow_id}/{runtime.run_number}/{runtime.command_id + 1}"
    return await WorkflowAwaitable(Command("START_CHILD", {
        "name": name,
        "args": list(args),
        "workflow_id": child_id,
        "parent_close_policy": selected.parent_close_policy,
    }))


async def continue_as_new(*args: Any) -> None:
    await WorkflowAwaitable(Command("CONTINUE_AS_NEW", {"args": list(args)}))


async def get_version(change_id: str, minimum: int, maximum: int) -> int:
    if minimum > maximum:
        raise ValueError("minimum version must not exceed maximum version")
    return await WorkflowAwaitable(Command("RECORD_VERSION", {
        "change_id": change_id,
        "minimum": minimum,
        "maximum": maximum,
        "version": maximum,
    }))


def info() -> dict[str, Any]:
    runtime = current_runtime()
    return {"workflow_id": runtime.workflow_id, "replaying": runtime.index < len(runtime.events)}


def now() -> datetime:
    runtime = current_runtime()
    event_index = min(max(runtime.index - 1, 0), len(runtime.events) - 1)
    return datetime.fromtimestamp(runtime.events[event_index].created_at, tz=timezone.utc)
