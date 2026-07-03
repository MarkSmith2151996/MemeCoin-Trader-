"""Paper trading execution adapter."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter


class PaperExecutionAdapter(ExecutionAdapter):
    def __init__(self, price_lookup: dict[str, float] | None = None) -> None:
        self._price_lookup = price_lookup or {}
        self._closed = False

    @property
    def mode(self) -> str:
        return "paper"

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        self._ensure_open()
        quote = await self.get_quote(mint_address, side, amount_sol, slippage_bps)
        return Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=quote.estimated_out_amount if side == Side.BUY else None,
            price_sol=quote.price_sol,
            slippage_bps=slippage_bps,
            mode=self.mode,
            status="simulated",
            metadata={"quote_provider": quote.provider},
        )

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        self._ensure_open()
        price = await self.get_current_price(mint_address) or 1.0
        estimated_out = amount_sol / price if side == Side.BUY else amount_sol * price
        return SwapQuote(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            estimated_out_amount=estimated_out,
            price_sol=price,
            price_impact_pct=0.0,
            slippage_bps=slippage_bps,
            provider=self.mode,
            expires_at=datetime.now(UTC) + timedelta(seconds=30),
        )

    async def get_current_price(self, mint_address: str) -> float | None:
        self._ensure_open()
        return self._price_lookup.get(mint_address)

    async def close(self) -> None:
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("paper execution adapter is closed")
