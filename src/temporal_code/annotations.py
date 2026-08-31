from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T", bound=type)


def _mark(obj: F, kind: str, name: str | None = None) -> F:
    setattr(obj, "__temporal_code_kind__", kind)
    setattr(obj, "__temporal_code_name__", name or obj.__name__)
    return obj


class _WorkflowAnnotations:
    @overload
    def defn(self, cls: T) -> T: ...

    @overload
    def defn(self, *, name: str | None = None) -> Callable[[T], T]: ...

    def defn(self, cls: T | None = None, *, name: str | None = None):
        def decorate(target: T) -> T:
            setattr(target, "__temporal_code_workflow__", name or target.__name__)
            return target

        return decorate(cls) if cls is not None else decorate

    def _method(self, kind: str, fn: F | None = None, *, name: str | None = None):
        def decorate(target: F) -> F:
            return _mark(target, kind, name)

        return decorate(fn) if fn is not None else decorate

    def run(self, fn: F | None = None, *, name: str | None = None):
        return self._method("run", fn, name=name)

    def signal(self, fn: F | None = None, *, name: str | None = None):
        return self._method("signal", fn, name=name)

    def query(self, fn: F | None = None, *, name: str | None = None):
        return self._method("query", fn, name=name)

    def update(self, fn: F | None = None, *, name: str | None = None):
        return self._method("update", fn, name=name)


class _ActivityAnnotations:
    def defn(self, fn: F | None = None, *, name: str | None = None):
        def decorate(target: F) -> F:
            return _mark(target, "activity", name)

        return decorate(fn) if fn is not None else decorate


workflow = _WorkflowAnnotations()
activity = _ActivityAnnotations()
