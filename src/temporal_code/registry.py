from __future__ import annotations

import inspect
from types import ModuleType
from typing import Any, Callable


class Registry:
    def __init__(self) -> None:
        self.workflows: dict[str, type] = {}
        self.activities: dict[str, Callable[..., Any]] = {}

    def register_workflow(self, cls: type) -> type:
        name = getattr(cls, "__temporal_code_workflow__", None)
        if not name:
            raise TypeError(f"{cls.__name__} is missing @workflow.defn")
        runs = [member for _, member in inspect.getmembers(cls) if getattr(member, "__temporal_code_kind__", None) == "run"]
        if len(runs) != 1 or not inspect.iscoroutinefunction(runs[0]):
            raise TypeError(f"{name} must have exactly one async @workflow.run method")
        asynchronous_handlers = [
            member.__name__
            for _, member in inspect.getmembers(cls)
            if getattr(member, "__temporal_code_kind__", None) in {"signal", "update"}
            and inspect.iscoroutinefunction(member)
        ]
        if asynchronous_handlers:
            raise TypeError(f"signal and update handlers must be synchronous: {asynchronous_handlers}")
        self.workflows[name] = cls
        return cls

    def register_activity(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        if getattr(fn, "__temporal_code_kind__", None) != "activity":
            raise TypeError(f"{fn.__name__} is missing @activity.defn")
        self.activities[getattr(fn, "__temporal_code_name__")] = fn
        return fn

    def discover(self, module: ModuleType) -> "Registry":
        for _, value in inspect.getmembers(module):
            if inspect.isclass(value) and getattr(value, "__temporal_code_workflow__", None):
                self.register_workflow(value)
            elif callable(value) and getattr(value, "__temporal_code_kind__", None) == "activity":
                self.register_activity(value)
        return self

    def workflow_methods(self, workflow_type: str, kind: str) -> dict[str, str]:
        cls = self.workflows[workflow_type]
        return {
            getattr(member, "__temporal_code_name__"): attr
            for attr, member in inspect.getmembers(cls)
            if getattr(member, "__temporal_code_kind__", None) == kind
        }

    def run_method(self, workflow_type: str) -> str:
        cls = self.workflows[workflow_type]
        return next(attr for attr, member in inspect.getmembers(cls) if getattr(member, "__temporal_code_kind__", None) == "run")

    def build_ids(self) -> list[str]:
        return sorted({
            build_id
            for workflow in self.workflows.values()
            if (build_id := getattr(workflow, "__temporal_code_build_id__", None))
        })
