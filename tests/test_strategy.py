import asyncio
from datetime import UTC, datetime, timedelta

from src.core.config import Settings
from src.core.models import CheckResult, Position, RiskAssessment, Side, Signal, SignalSource, SignalType, TokenInfo, Trade
from src.strategy.decision_engine import DecisionEngine
from src.strategy.exits import DynamicExitState, build_partial_exits, evaluate_exits
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


class RecordingCallableRiskScorer:
    def __init__(self, assessment: RiskAssessment) -> None:
        self.assessment = assessment
        self.received_config = None

    async def __call__(self, signal_or_token, config=None) -> RiskAssessment:
        self.received_config = config
        return self.assessment


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


def settings_with_liquidity_sizing(*, enabled: bool, max_single_position_sol: float = 0.5) -> Settings:
    settings = Settings()
    return settings.model_copy(
        update={
            "position": settings.position.model_copy(
                update={
                    "liquidity_sizing_enabled": enabled,
                    "max_single_position_sol": max_single_position_sol,
                }
            )
        }
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
        assert "market_regime" not in trade.metadata
        assert (await manager.get_position("mint")) is not None

    asyncio.run(run())


def test_decision_engine_keeps_fixed_sizing_when_liquidity_sizing_disabled() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=False)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(
            update={"token": TokenInfo(mint_address="mint", liquidity_sol=30.0)}
        )
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                weight=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.4
        assert decision.trade.metadata["position_sizing_mode"] == "flat"
        assert decision.trade.metadata["position_sizing_reason"] == "liquidity_sizing_disabled"
        assert decision.trade.metadata["position_sizing_max_position_cap_sol"] == 0.5

    asyncio.run(run())


def test_decision_engine_ignores_discovery_edge_diagnostics_for_strict_execution() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(safe_assessment()), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                payload={
                    "edge_score": 100,
                    "edge_breakdown": "src=3/comp=1.00 mode=launch attn=100/present",
                },
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.4
        assert "edge_score" not in decision.trade.metadata
        assert len(adapter.executed) == 1
        assert adapter.executed[0].amount_sol == decision.trade.amount_sol

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


def test_decision_engine_skips_unknown_liquidity_when_liquidity_sizing_enabled() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=True)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(update={"token": TokenInfo(mint_address="mint")})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=1.0,
            )
        )

        assert decision.trade is None
        assert decision.rejection_reason == "liquidity_sizing_liquidity_unknown"
        assert decision.metadata["position_sizing_skip_trade"] is True
        assert decision.metadata["position_sizing_reason"] == "liquidity_unknown"
        assert decision.metadata["position_sizing_max_position_cap_sol"] == 0.0

    asyncio.run(run())


def test_decision_engine_reports_failed_check_reason() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        blocked = safe_assessment().model_copy(update={"honeypot_check": CheckResult.FAIL, "reasons": ["honeypot"]})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(blocked), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
            )
        )

        assert decision.trade is None
        assert decision.rejection_reason == "honeypot_check_failed"

    asyncio.run(run())


def test_decision_engine_caps_15_to_50_sol_liquidity_to_point_two_five() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=True, max_single_position_sol=0.5)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(
            update={"token": TokenInfo(mint_address="mint", liquidity_sol=30.0)}
        )
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.25
        assert decision.trade.metadata["position_sizing_reason"] == "15_to_50_sol"
        assert decision.trade.metadata["position_sizing_max_position_cap_sol"] == 0.25
        assert decision.trade.metadata["position_sizing_capped"] is True

    asyncio.run(run())


def test_decision_engine_caps_50_to_200_sol_liquidity_to_point_five() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=True, max_single_position_sol=1.0)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(
            update={"token": TokenInfo(mint_address="mint", liquidity_sol=75.0)}
        )
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                weight=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.5
        assert decision.trade.metadata["position_sizing_reason"] == "50_to_200_sol"
        assert decision.trade.metadata["position_sizing_max_position_cap_sol"] == 0.5

    asyncio.run(run())


def test_decision_engine_never_exceeds_global_cap_for_high_liquidity() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=True, max_single_position_sol=0.75)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(
            update={"token": TokenInfo(mint_address="mint", liquidity_sol=250.0)}
        )
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.75
        assert decision.trade.metadata["position_sizing_reason"] == "over_200_sol"
        assert decision.trade.metadata["position_sizing_max_position_cap_sol"] == 0.75
        assert decision.trade.metadata["position_sizing_helper_max_position_sol"] == 1.0

    asyncio.run(run())


def test_decision_engine_passes_runtime_risk_config_to_callable_signal_scorer() -> None:
    async def run() -> None:
        settings = Settings().model_copy(update={"risk": Settings().risk.model_copy(update={"min_age_minutes": 0})})
        adapter = FakeExecutionAdapter({"mint": 0.25})
        scorer = RecordingCallableRiskScorer(safe_assessment())
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, scorer, manager, settings)

        trade = await engine.evaluate_signal(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
            )
        )

        assert trade is not None
        assert scorer.received_config == settings.risk

    asyncio.run(run())


def test_decision_engine_returns_sizing_metadata_for_paper_decisions() -> None:
    async def run() -> None:
        settings = settings_with_liquidity_sizing(enabled=True)
        adapter = FakeExecutionAdapter({"mint": 0.25})
        assessment = safe_assessment().model_copy(
            update={"token": TokenInfo(mint_address="mint", liquidity_sol=30.0)}
        )
        manager = PositionManager(None, settings)
        engine = DecisionEngine(adapter, FakeRiskScorer(assessment), manager, settings)

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.metadata["position_sizing_reason"] == "15_to_50_sol"
        assert decision.trade.metadata["position_sizing_liquidity_sol"] == 30.0
        assert decision.trade.metadata["position_sizing_mode"] == "liquidity"
        assert "market_regime" not in decision.trade.metadata

    asyncio.run(run())


def test_decision_engine_adds_market_regime_metadata_when_enabled() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(
            adapter,
            FakeRiskScorer(safe_assessment()),
            manager,
            settings,
            market_regime_enabled=True,
        )

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                weight=1.0,
                payload={
                    "newPoolCount": 12,
                    "averageLiquiditySol": 95.0,
                    "medianVolumeSol": 180.0,
                    "medianTransactionCount": 140,
                    "paperTradeSuccessRate": 0.7,
                    "paperTradeSampleSize": 10,
                    "signalCount": 14,
                    "signalVelocityPerHour": 6.5,
                },
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.4
        assert decision.metadata["market_regime_enabled"] is True
        assert decision.metadata["market_regime"] == "hot"
        assert decision.metadata["market_regime_confidence"] == 0.9
        assert decision.metadata["market_regime_reasons"] == [
            "high_signal_count",
            "high_signal_velocity",
            "healthy_liquidity",
            "healthy_flow",
        ]
        assert decision.trade.metadata["market_regime"] == "hot"
        assert decision.trade.metadata["market_regime_adjustment_hints"]["risk_appetite"] == "measured"

    asyncio.run(run())


def test_decision_engine_market_regime_unknown_degrades_safely_when_enabled() -> None:
    async def run() -> None:
        settings = Settings()
        adapter = FakeExecutionAdapter({"mint": 0.25})
        manager = PositionManager(None, settings)
        engine = DecisionEngine(
            adapter,
            FakeRiskScorer(safe_assessment()),
            manager,
            settings,
            market_regime_enabled=True,
        )

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.MANUAL,
                type=SignalType.BUY,
                mint_address="mint",
                confidence=0.8,
                weight=1.0,
            )
        )

        assert decision.trade is not None
        assert decision.trade.amount_sol == 0.4
        assert decision.trade.metadata["market_regime"] == "unknown"
        assert decision.trade.metadata["market_regime_confidence"] == 0.2
        assert decision.trade.metadata["market_regime_reasons"] == ["insufficient_activity_data"]

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


def test_evaluate_exits_preserves_existing_take_profit_behavior_when_dynamic_exits_disabled() -> None:
    settings = Settings()
    position = Position(
        mint_address="mint",
        entry_trade_id="trade-1",
        amount_sol=0.5,
        token_amount=2.0,
        entry_price_sol=1.0,
        partial_exits=build_partial_exits(settings.exits),
    )
    observed_at = datetime.now(UTC)

    actions = evaluate_exits(
        position,
        current_price=5.0,
        pool_liquidity_sol=100.0,
        config=settings,
        dynamic_state=DynamicExitState(
            current_volume=10.0,
            peak_volume=100.0,
            volume_below_threshold_started_at=observed_at - timedelta(minutes=20),
            reference_liquidity=200.0,
            reference_liquidity_at=observed_at - timedelta(seconds=30),
            observed_at=observed_at,
        ),
    )

    assert len(actions) == 2
    assert [action.reason for action in actions] == [
        "take profit hit 2.0x entry",
        "take profit hit 5.0x entry",
    ]
