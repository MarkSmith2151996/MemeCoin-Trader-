import asyncio
from datetime import UTC, datetime, timedelta

from src.core.config import Settings
from src.core.models import CheckResult, Position, RiskAssessment, Side, Signal, SignalSource, SignalType, TokenInfo, Trade
from src.strategy.decision_engine import DecisionEngine
from src.strategy.exits import build_partial_exits, evaluate_exits
from src.strategy.position_manager import PositionManager


class FakeExecutionAdapter:
    def __init__(self, price_map: dict[str, float]) -> None:
        self._price_map = price_map
        self.executed: list[Trade] = []

    async def execute_swap(self, mint_address: str, side: Side, amount_sol: float, slippage_bps: int = 300) -> Trade:
        price = self._price_map[mint_address]
        trade = Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=amount_sol / price,
            price_sol=price,
            slippage_bps=slippage_bps,
            mode=self.mode,
        )
        self.executed.append(trade)
        return trade

    async def get_quote(self, mint_address: str, side: Side, amount_sol: float, slippage_bps: int = 300):
        raise NotImplementedError

    async def get_current_price(self, mint_address: str) -> float | None:
        return self._price_map.get(mint_address)

    async def close(self) -> None:
        return None

    @property
    def mode(self) -> str:
        return "paper"


class FakeRiskScorer:
    def __init__(self, assessment: RiskAssessment) -> None:
        self.assessment = assessment

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        return self.assessment

    async def assess_token(self, token: TokenInfo, config) -> RiskAssessment:
        return self.assessment.model_copy(update={"token": token})


def safe_assessment() -> RiskAssessment:
    return RiskAssessment(
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.PASS,
        reasons=[],
        score=100.0,
    )


def test_decision_engine_executes_risk_gated_buy() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(safe_assessment()), manager, settings)

        trade = await engine.evaluate_signal(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                weight=1.0,
            )
        )

        assert trade is not None
        assert trade.amount_sol == 0.4
        assert trade.side == Side.BUY
        assert (await manager.get_position("mint")) is not None

    asyncio.run(run())


def test_decision_engine_blocks_failed_risk() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        blocked = safe_assessment().model_copy(update={"honeypot_check": CheckResult.FAIL, "reasons": ["honeypot"]})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(blocked), manager, settings)

        trade = await engine.evaluate_signal(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
            )
        )

        assert trade is None
        assert adapter.executed == []

    asyncio.run(run())


def test_evaluate_exits_sells_remaining_at_20x() -> None:
    settings = Settings()
    position = Position(
        mint_address="mint",
        entry_trade_id="trade-1",
        amount_sol=0.5,
        token_amount=2.0,
        entry_price_sol=0.25,
        partial_exits=build_partial_exits(settings.exits),
    )

    actions = evaluate_exits(position, current_price=5.0, pool_liquidity_sol=100.0, config=settings)

    assert len(actions) == 4
    assert actions[-1].is_full_exit is True
    assert actions[-1].sell_pct == 0.25


def test_evaluate_exits_time_stop_full_exits_stalled_position() -> None:
    settings = Settings()
    position = Position(
        mint_address="mint",
        entry_trade_id="trade-1",
        amount_sol=0.5,
        token_amount=2.0,
        entry_price_sol=1.0,
        opened_at=datetime.now(UTC) - timedelta(minutes=121),
        partial_exits=build_partial_exits(settings.exits),
    )

    actions = evaluate_exits(position, current_price=1.5, pool_liquidity_sol=100.0, config=settings)

    assert len(actions) == 1
    assert actions[0].is_full_exit is True
