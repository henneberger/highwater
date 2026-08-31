from __future__ import annotations

import copy
import inspect
import traceback
from typing import Any

from .errors import (
    ActivityError,
    ChildWorkflowError,
    NonDeterminismError,
    QueryNotFound,
    UpdateNotFound,
    WorkflowCancelled,
)
from .model import ActivationResult, Command, Event
from .registry import Registry
from .runtime import ReplayRuntime


class WorkflowRunner:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

    def invoke_handler(self, instance: Any, kind: str, name: str, args: list[Any]) -> Any:
        workflow_type = getattr(type(instance), "__highwater_workflow__")
        methods = self.registry.workflow_methods(workflow_type, kind)
        if name not in methods:
            if kind == "signal":
                return None
            raise UpdateNotFound(name)
        return getattr(instance, methods[name])(*args)

    async def activate(
        self,
        workflow_id: str,
        workflow_type: str,
        events: list[Event],
    ) -> ActivationResult:
        instance = self.registry.workflows[workflow_type]()
        runtime = ReplayRuntime(self, workflow_id, instance, events)
        coroutine = getattr(instance, self.registry.run_method(workflow_type))(*events[0].data["args"])
        token = runtime.bind()
        value: Any = None
        raised: BaseException | None = None
        blocked = False
        try:
            while True:
                try:
                    yielded = coroutine.throw(raised) if raised is not None else coroutine.send(value)
                    raised = None
                    value = None
                except StopIteration as completed:
                    runtime.pending.append(Command("COMPLETE_WORKFLOW", {"result": completed.value}))
                    break
                if not isinstance(yielded, Command):
                    raise RuntimeError("workflow awaited a non-deterministic operation")
                try:
                    ready, value = runtime.resolve(yielded)
                except WorkflowCancelled:
                    runtime.pending.append(Command("CANCEL_WORKFLOW", {}))
                    blocked = True
                    break
                except (ActivityError, ChildWorkflowError) as error:
                    raised = error
                    continue
                if not ready:
                    blocked = True
                    break
        except WorkflowCancelled:
            runtime.pending.append(Command("CANCEL_WORKFLOW", {}))
            blocked = True
        except NonDeterminismError:
            raise
        except Exception as error:
            runtime.pending.append(Command("FAIL_WORKFLOW", {
                "error": "".join(traceback.format_exception_only(type(error), error)).strip(),
            }))
        finally:
            runtime.unbind(token)
            if blocked:
                coroutine.close()
        return ActivationResult(runtime.pending, blocked, instance, events[-1].id)

    async def query(
        self,
        workflow_id: str,
        workflow_type: str,
        events: list[Event],
        name: str,
        args: list[Any],
    ) -> Any:
        activation = await self.activate(workflow_id, workflow_type, events)
        methods = self.registry.workflow_methods(workflow_type, "query")
        if name not in methods:
            raise QueryNotFound(name)
        before = copy.deepcopy(vars(activation.instance))
        value = getattr(activation.instance, methods[name])(*args)
        if inspect.isawaitable(value):
            value = await value
        if vars(activation.instance) != before:
            raise RuntimeError("query handlers must not mutate workflow state")
        return value
