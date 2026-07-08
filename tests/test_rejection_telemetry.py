import asyncio
from datetime import UTC, datetime, timedelta

from src.core.config import Settings
from src.core.models import CheckResult, RiskAssessment, Side, Signal, SignalSource, SignalType, TokenInfo, Trade
from src.strategy.decision_engine import DecisionEngine, RejectionRecord
from src.strategy.position_manager import PositionManager


class FakeExecutionAdapter:
    def __init__(self, price_map: dict[str, float]) -> None:
        self._price_map = price_map

    async def execute_swap(self, mint_address: str, side: Side, amount_sol: float, slippage_bps: int = 300) -> Trade:
        price = self._price_map[mint_address]
        return Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=amount_sol / price,
            price_sol=price,
            slippage_bps=slippage_bps,
            mode=self.mode,
        )

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


def _safe_assessment() -> RiskAssessment:
    token = TokenInfo(
        mint_address="mint",
        liquidity_sol=20.0,
        top10_holder_pct=25.0,
        creator_holding_pct=5.0,
        unique_buyers=30,
        mint_authority_revoked=True,
        freeze_authority_revoked=True,
        created_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    return RiskAssessment(
        token=token,
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.PASS,
        score=100.0,
        reasons=[],
    )


def test_passing_candidate_records_passed_outcome() -> None:
    async def run() -> None:
        settings = Settings()
        engine = DecisionEngine(
            FakeExecutionAdapter({"mint": 0.25}),
            FakeRiskScorer(_safe_assessment()),
            PositionManager(None, settings),
            settings,
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

        record = decision.metadata["rejection_record"]
        assert decision.trade is not None
        assert record["outcome"] == "passed"
        assert record["failed_check"] is None
        assert record["check_results"]["liquidity_check"]["passed"] is True

    asyncio.run(run())


def test_failing_candidate_records_first_failed_check() -> None:
    async def run() -> None:
        settings = Settings()
        blocked = _safe_assessment().model_copy(
            update={
                "token": _safe_assessment().token.model_copy(update={"liquidity_sol": 4.0}),
                "liquidity_check": CheckResult.FAIL,
                "top10_holder_check": CheckResult.FAIL,
                "reasons": ["liquidity_check failed", "top10_holder_check failed"],
            }
        )
        engine = DecisionEngine(
            FakeExecutionAdapter({"mint": 0.25}),
            FakeRiskScorer(blocked),
            PositionManager(None, settings),
            settings,
        )

        decision = await engine.evaluate_signal_with_diagnostics(
            Signal(
                source=SignalSource.PUMP_FUN,
                type=SignalType.NEW_POOL,
                mint_address="mint",
                confidence=0.9,
                weight=1.0,
            )
        )

        record = decision.metadata["rejection_record"]
        assert decision.trade is None
        assert decision.rejection_reason == "liquidity_check_failed"
        assert record["outcome"] == "rejected"
        assert record["failed_check"] == "liquidity_check"
        assert record["check_results"]["liquidity_check"]["value"] == 4.0
        assert record["check_results"]["liquidity_check"]["threshold"] == settings.risk.min_liquidity_sol

    asyncio.run(run())


def test_summarize_rejections_returns_correct_totals_and_distributions() -> None:
    now = datetime.now(UTC)
    records = [
        RejectionRecord(
            mint_address="mint-a",
            signal_source="pump_fun",
            signal_strength=0.8,
            timestamp=now,
            outcome="passed",
            failed_check=None,
            check_results={},
        ),
        RejectionRecord(
            mint_address="mint-b",
            signal_source="pump_fun",
            signal_strength=0.7,
            timestamp=now,
            outcome="rejected",
            failed_check="liquidity_check",
            check_results={},
        ),
        RejectionRecord(
            mint_address="mint-c",
            signal_source="whale_tracker",
            signal_strength=0.6,
            timestamp=now,
            outcome="rejected",
            failed_check="top10_holder_check",
            check_results={},
        ),
    ]

    summary = DecisionEngine.summarize_rejections(records)

    assert summary["total_evaluated"] == 3
    assert summary["passed_count"] == 1
    assert summary["rejected_count"] == 2
    assert summary["rejection_reason_distribution"]["liquidity_check"] == 1
    assert summary["rejection_reason_distribution"]["top10_holder_check"] == 1
    assert summary["source_distribution"]["pump_fun"] == 2
    assert summary["source_distribution"]["whale_tracker"] == 1
