"""Signal source interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.core.models import Signal


class SignalSource(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def poll(self) -> list[Signal]: ...
