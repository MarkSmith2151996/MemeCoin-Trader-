"""Live Pump.fun execution router with Jupiter fallback after graduation."""

from __future__ import annotations

import logging

from src.chain.pumpfun import CurveCompleteError
from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter
from src.execution.direct import DirectExecutor
from src.execution.live import LiveExecutionAdapter

log = logging.getLogger("pumpfun_router")


class PumpFunExecutionRouter(ExecutionAdapter):
    """Keep direct Pump fills in the existing live adapter lifecycle."""

    def __init__(self, jupiter: LiveExecutionAdapter, direct: DirectExecutor) -> None:
        self._jupiter = jupiter
        self._direct = direct

    @property
    def mode(self) -> str:
        return "live"

    async def buy_bonding_curve(
        self,
        mint_address: str,
        amount_sol: float,
        slippage_bps: int,
    ) -> Trade:
        """Use direct only when the discovery signal and current curve agree."""

        await self._jupiter.validate_direct_buy(mint_address, amount_sol)
        if await self._direct.has_active_curve(mint_address):
            log.info("LIVE BUY [direct] mint=%s", mint_address[:16])
            try:
                return self._tag(
                    await self._direct.execute_swap(
                        mint_address,
                        Side.BUY,
                        amount_sol,
                        slippage_bps,
                    ),
                    "direct",
                )
            except CurveCompleteError:
                # The curve completed after the preflight read; no transaction was sent.
                log.info("LIVE BUY [jupiter] mint=%s direct_curve_completed", mint_address[:16])
        else:
            log.info("LIVE BUY [jupiter] mint=%s direct_curve_unavailable", mint_address[:16])
        return self._tag(await self._jupiter.buy(mint_address, amount_sol, slippage_bps), "jupiter")

    async def buy(self, mint_address: str, amount_sol: float, slippage_bps: int = 100) -> Trade:
        log.info("LIVE BUY [jupiter] mint=%s", mint_address[:16])
        return self._tag(await self._jupiter.buy(mint_address, amount_sol, slippage_bps), "jupiter")

    async def sell(self, mint_address: str, token_amount: float, slippage_bps: int = 100) -> Trade:
        if await self._direct.has_active_curve(mint_address):
            log.info("LIVE SELL [direct] mint=%s", mint_address[:16])
            try:
                trade = self._tag(
                    await self._direct.execute_swap(
                        mint_address,
                        Side.SELL,
                        token_amount,
                        slippage_bps,
                    ),
                    "direct",
                )
            except CurveCompleteError:
                log.info("LIVE SELL [jupiter] mint=%s direct_curve_completed", mint_address[:16])
            except Exception as exc:
                self._jupiter.trip_circuit_breaker(
                    error=f"direct sell failed: {exc}",
                    mint=mint_address,
                    reason="sell_failure",
                )
                raise
            else:
                trade.metadata["token_balance_after"] = (
                    await self._jupiter.verify_token_balance_cleared(mint_address)
                )
                return trade
        else:
            log.info("LIVE SELL [jupiter] mint=%s direct_curve_unavailable", mint_address[:16])
        return self._tag(
            await self._jupiter.sell(mint_address, token_amount, slippage_bps),
            "jupiter",
        )

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        if side == Side.BUY:
            return await self.buy(mint_address, amount_sol, slippage_bps)
        return await self.sell(mint_address, amount_sol, slippage_bps)

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        return await self._jupiter.get_quote(mint_address, side, amount_sol, slippage_bps)

    async def get_current_price(self, mint_address: str) -> float | None:
        return await self._jupiter.get_current_price(mint_address)

    async def get_token_balance(self, mint_address: str) -> float | None:
        return await self._jupiter.get_token_balance(mint_address)

    async def get_wallet_holdings(self) -> dict[str, float] | None:
        return await self._jupiter.get_wallet_holdings()

    async def get_sol_balance(self) -> float | None:
        return await self._jupiter.get_sol_balance()

    def trip_circuit_breaker(self, **kwargs: object) -> None:
        self._jupiter.trip_circuit_breaker(**kwargs)  # type: ignore[arg-type]

    async def close(self) -> None:
        await self._direct.close()
        await self._jupiter.close()

    @staticmethod
    def _tag(trade: Trade, path: str) -> Trade:
        trade.mode = "live"
        trade.metadata = {**trade.metadata, "execution_path": path}
        return trade
