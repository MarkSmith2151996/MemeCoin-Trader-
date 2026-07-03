"""Twitter/X signal source placeholder."""

from __future__ import annotations

from src.core.models import Signal
from src.signals.base import SignalSource


class TwitterSignalSource(SignalSource):
    @property
    def name(self) -> str:
        return "twitter"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def poll(self) -> list[Signal]:
        return []
