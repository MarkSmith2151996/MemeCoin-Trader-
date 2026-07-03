"""Aggregate signals from multiple async sources."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from src.core.models import Signal
from src.signals.base import SignalSource


class SignalAggregator:
    def __init__(self, sources: Iterable[SignalSource] = ()) -> None:
        self.sources = list(sources)

    async def start(self) -> None:
        await asyncio.gather(*(source.start() for source in self.sources))

    async def stop(self) -> None:
        await asyncio.gather(*(source.stop() for source in self.sources))

    async def poll(self) -> list[Signal]:
        batches = await asyncio.gather(*(source.poll() for source in self.sources))
        return [signal for batch in batches for signal in batch]
