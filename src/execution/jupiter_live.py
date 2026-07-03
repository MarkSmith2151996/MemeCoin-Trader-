"""Live Jupiter execution adapter boundary."""

from __future__ import annotations

from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter


class JupiterLiveExecutionAdapter(ExecutionAdapter):
    @property
    def mode(self) -> str:
        return "live"

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        raise NotImplementedError("Live swaps must be implemented behind risk gates")

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        raise NotImplementedError("Live Jupiter quotes are a Phase 2 task")

    async def get_current_price(self, mint_address: str) -> float | None:
        raise NotImplementedError("Live price lookup is a Phase 2 task")

    async def close(self) -> None:
        return None
