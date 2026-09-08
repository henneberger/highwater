from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client


@dataclass(frozen=True)
class ViewRow:
    key: str | None
    value: Any
    count: int


@dataclass(frozen=True)
class ViewSnapshot:
    snapshot_id: str
    created_at: float
    consistency: str
    input_positions: dict[str, list[dict[str, Any]]]
    views: dict[str, tuple[ViewRow, ...]]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ViewSnapshot:
        return cls(
            snapshot_id=value["snapshot_id"],
            created_at=value["created_at"],
            consistency=value["consistency"],
            input_positions=value["input_positions"],
            views={name: tuple(ViewRow(**row) for row in rows)
                   for name, rows in value["views"].items()},
        )

    def rows(self, operator_id: str, *, key: str | None = None) -> tuple[ViewRow, ...]:
        rows = self.views[operator_id]
        return rows if key is None else tuple(row for row in rows if row.key == key)


@dataclass(frozen=True)
class MaterializedView:
    client: Client
    operator_id: str

    def __post_init__(self) -> None:
        if not self.operator_id.strip():
            raise ValueError("operator_id must not be empty")

    async def snapshot(self) -> ViewSnapshot:
        """Save a named cut. Delete it explicitly when it is no longer needed."""
        return await self.client.snapshot_views(self.operator_id)

    async def get(self, key: str) -> tuple[ViewRow, ...]:
        """Read this key's current rows, releasing the temporary saved snapshot."""
        snapshot = await self.snapshot()
        try:
            return snapshot.rows(self.operator_id, key=key)
        finally:
            await self.client.delete_view_snapshot(snapshot.snapshot_id)
