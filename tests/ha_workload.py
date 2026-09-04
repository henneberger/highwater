"""Application used by the opt-in multi-node HA test."""
import asyncio
from dataclasses import dataclass

from highwater import streaming


@streaming.process(key="key", build_id="chaos-v1")
@dataclass
class HACounter:
    total: int = 0

    @streaming.event
    async def apply(self, event):
        await asyncio.sleep(0.03)
        self.total += event.delta
        return {"key": event.key, "total": self.total}
