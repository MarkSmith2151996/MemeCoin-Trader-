"""Paper adapter with an executor-owned current candidate mark."""

from __future__ import annotations

from src.core.config import PaperFrictionConfig
from src.core.models import Side, Trade
from src.execution.paper_friction import apply_entry_friction
from src.execution.paper import PaperExecutionAdapter as BasePaperExecutionAdapter


class PaperExecutionAdapter(BasePaperExecutionAdapter):
    """Allow the database candidate price to drive deterministic paper fills."""

    def __init__(self, friction: PaperFrictionConfig | None = None) -> None:
        super().__init__()
        self._friction = friction or PaperFrictionConfig()

    def set_price(self, mint_address: str, price_sol: float) -> None:
        if price_sol <= 0:
            raise ValueError("paper candidate price must be positive")
        self._price_lookup[mint_address] = price_sol

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
        *,
        pool_sol: float | None = None,
    ) -> Trade:
        trade = await super().execute_swap(mint_address, side, amount_sol, slippage_bps)
        if side != Side.BUY or pool_sol is None or trade.token_amount is None:
            return trade
        fill = apply_entry_friction(trade.token_amount, amount_sol, float(pool_sol), self._friction)
        trade.token_amount = fill.token_amount
        trade.metadata = {
            **trade.metadata,
            "quoted_token_amount": fill.quoted_token_amount,
            "slippage_entry_pct": fill.slippage_pct,
            "dex_fee_entry_sol": fill.dex_fee_sol,
            "priority_fee_buy_sol": self._friction.priority_fee_buy_sol,
        }
        return trade
