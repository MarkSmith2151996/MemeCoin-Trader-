from __future__ import annotations

import asyncio

from src.chain.pumpfun import CurveCompleteError
from src.core.models import Side, Trade
from src.execution.direct import DirectPriceImpactExceeded
from src.execution.pumpfun_router import PumpFunExecutionRouter


class _FakeJupiter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def validate_direct_buy(self, mint: str, amount: float) -> None:
        self.calls.append("validate")

    async def buy(
        self,
        mint: str,
        amount: float,
        slippage: int,
        *,
        prevalidated: bool = False,
    ) -> Trade:
        assert prevalidated is True
        self.calls.append("buy")
        return Trade(
            mint_address=mint,
            side=Side.BUY,
            amount_sol=amount,
            token_amount=1,
            price_sol=amount,
        )

    async def sell(self, mint: str, amount: float, slippage: int) -> Trade:
        self.calls.append("sell")
        return Trade(
            mint_address=mint,
            side=Side.SELL,
            amount_sol=amount,
            token_amount=1,
            price_sol=amount,
        )

    async def verify_token_balance_cleared(self, mint: str) -> float:
        self.calls.append("verify_clear")
        return 0.0

    def trip_circuit_breaker(self, **kwargs) -> None:
        self.calls.append("trip")

    async def close(self) -> None:
        self.calls.append("close")


class _FakeDirect:
    def __init__(self, active: bool, completes: bool = False, impact_blocked: bool = False) -> None:
        self.active = active
        self.completes = completes
        self.impact_blocked = impact_blocked
        self.calls: list[str] = []

    async def has_active_curve(self, mint: str) -> bool:
        self.calls.append("check")
        return self.active

    async def execute_swap(self, mint: str, side: Side, amount: float, slippage: int) -> Trade:
        self.calls.append(side.value)
        if self.completes:
            raise CurveCompleteError("completed")
        if self.impact_blocked:
            raise DirectPriceImpactExceeded("direct Pump price impact 6.00% exceeds 5.00%")
        return Trade(
            mint_address=mint,
            side=side,
            amount_sol=amount,
            token_amount=1,
            price_sol=amount,
        )

    async def close(self) -> None:
        self.calls.append("close")


def test_bonding_buy_uses_direct_and_keeps_live_mode() -> None:
    async def run() -> None:
        jupiter = _FakeJupiter()
        direct = _FakeDirect(active=True)
        router = PumpFunExecutionRouter(jupiter, direct)  # type: ignore[arg-type]

        trade = await router.buy_bonding_curve("mint", 0.02, 300)

        assert jupiter.calls == ["validate"]
        assert direct.calls == ["check", "BUY"]
        assert trade.mode == "live"
        assert trade.metadata["execution_path"] == "direct"

    asyncio.run(run())


def test_unavailable_or_completed_curve_falls_back_to_jupiter() -> None:
    async def run() -> None:
        jupiter = _FakeJupiter()
        unavailable = PumpFunExecutionRouter(jupiter, _FakeDirect(active=False))  # type: ignore[arg-type]
        trade = await unavailable.buy_bonding_curve("mint", 0.02, 300)
        assert trade.metadata["execution_path"] == "jupiter"
        assert jupiter.calls == ["validate", "buy"]

        jupiter = _FakeJupiter()
        completed = PumpFunExecutionRouter(jupiter, _FakeDirect(active=True, completes=True))  # type: ignore[arg-type]
        trade = await completed.buy_bonding_curve("mint", 0.02, 300)
        assert trade.metadata["execution_path"] == "jupiter"
        assert jupiter.calls == ["validate", "buy"]

    asyncio.run(run())


def test_direct_price_impact_guard_falls_back_to_jupiter() -> None:
    async def run() -> None:
        jupiter = _FakeJupiter()
        direct = _FakeDirect(active=True, impact_blocked=True)
        router = PumpFunExecutionRouter(jupiter, direct)  # type: ignore[arg-type]

        trade = await router.buy_bonding_curve("mint", 0.02, 300)

        assert direct.calls == ["check", "BUY"]
        assert jupiter.calls == ["validate", "buy"]
        assert trade.metadata["execution_path"] == "jupiter"

    asyncio.run(run())


def test_sell_rechecks_curve_and_falls_back_when_graduated() -> None:
    async def run() -> None:
        jupiter = _FakeJupiter()
        router = PumpFunExecutionRouter(jupiter, _FakeDirect(active=False))  # type: ignore[arg-type]

        trade = await router.sell("mint", 42, 300)

        assert jupiter.calls == ["sell"]
        assert trade.metadata["execution_path"] == "jupiter"

    asyncio.run(run())


def test_direct_sell_defers_wallet_balance_cleanup() -> None:
    async def run() -> None:
        jupiter = _FakeJupiter()
        direct = _FakeDirect(active=True)
        router = PumpFunExecutionRouter(jupiter, direct)  # type: ignore[arg-type]

        trade = await router.sell("mint", 42, 300)

        assert direct.calls == ["check", "SELL"]
        assert jupiter.calls == []
        assert trade.mode == "live"
        assert trade.metadata["execution_path"] == "direct"

    asyncio.run(run())
