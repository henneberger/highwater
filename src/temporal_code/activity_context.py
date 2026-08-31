from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ActivityContext:
    task_id: int
    workflow_id: str
    attempt: int
    heartbeat_callback: Callable[[Any], bool]
    cancelled_callback: Callable[[], bool]

    def heartbeat(self, details: Any = None) -> None:
        if not self.heartbeat_callback(details):
            raise RuntimeError("activity was cancelled or its lease was lost")

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled_callback()


_context: contextvars.ContextVar[ActivityContext] = contextvars.ContextVar("temporal_code_activity")


def current_activity() -> ActivityContext:
    try:
        return _context.get()
    except LookupError as error:
        raise RuntimeError("not executing an activity") from error


def heartbeat(details: Any = None) -> None:
    current_activity().heartbeat(details)
