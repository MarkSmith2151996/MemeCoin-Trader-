"""Execution adapter contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.core.models import Side, SwapQuote, Trade


class ExecutionAdapter(ABC):
    @abstractmethod
    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade: ...

    @abstractmethod
    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote: ...

    @abstractmethod
    async def get_current_price(self, mint_address: str) -> float | None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @property
    @abstractmethod
    def mode(self) -> str: ...
