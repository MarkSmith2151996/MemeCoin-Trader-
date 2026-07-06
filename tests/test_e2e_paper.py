import asyncio
from pathlib import Path

from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import (
    CheckResult,
    PositionStatus,
    RiskAssessment,
    Side,
    Signal,
    SignalSource,
    SignalType,
    SwapQuote,
    TokenInfo,
    Trade,
)
from src.execution.base import ExecutionAdapter
from src.monitoring.dashboard import (
    load_dashboard_snapshot,
    load_open_positions,
    load_recent_trades,
    run_dashboard,
)
from src.strategy.decision_engine import DecisionEngine
from src.strategy.position_manager import PositionManager


FAKE_MINT = "FakeMint1111111111111111111111111111111111111"
SMOKE_PRICE_SOL = 0.00001


class SmokePaperExecutionAdapter(ExecutionAdapter):
    def __init__(self, price_sol: float = SMOKE_PRICE_SOL) -> None:
        self.price_sol = price_sol
        self.executed: list[Trade] = []

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        trade = Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=amount_sol / self.price_sol,
            price_sol=self.price_sol,
            slippage_bps=slippage_bps,
            tx_signature=f"PAPER-SMOKE-{len(self.executed) + 1}",
            mode=self.mode,
            status="simulated",
            metadata={"strategy": "offline_e2e_smoke", "notes": "CT-090 offline paper smoke"},
        )
        self.executed.append(trade)
        return trade

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        return SwapQuote(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            estimated_out_amount=amount_sol / self.price_sol,
            price_sol=self.price_sol,
            slippage_bps=slippage_bps,
            provider="paper-smoke",
        )

    async def get_current_price(self, mint_address: str) -> float | None:
        return self.price_sol

    async def close(self) -> None:
        return None

    @property
    def mode(self) -> str:
        return "paper"


class PassingRiskScorer:
    def __init__(self, assessment: RiskAssessment) -> None:
        self.assessment = assessment
        self.calls: list[Signal] = []

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        self.calls.append(signal)
        return self.assessment.model_copy(
            update={
                "token": self.assessment.token
                or TokenInfo(mint_address=signal.mint_address, liquidity_sol=100.0),
            }
        )


def build_passing_assessment(mint_address: str) -> RiskAssessment:
    return RiskAssessment(
        token=TokenInfo(
            mint_address=mint_address,
            liquidity_sol=100.0,
            unique_buyers=250,
            top10_holder_pct=12.0,
            creator_holding_pct=2.5,
            mint_authority_revoked=True,
            freeze_authority_revoked=True,
        ),
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.PASS,
        score=0.0,
        reasons=[],
    )


def build_signal() -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=FAKE_MINT,
        confidence=0.8,
        payload={
            "offline_smoke": True,
            "test_case": "CT-090",
            "mint_address": FAKE_MINT,
        },
    )


def expected_position_size(signal: Signal, settings_max_single_sol: float, settings_max_portfolio_sol: float) -> float:
    capped_single = min(settings_max_single_sol, settings_max_portfolio_sol)
    return round(capped_single * signal.confidence * signal.weight, 6)


def test_offline_e2e_paper_smoke(tmp_path: Path) -> None:
    async def run() -> None:
        settings = load_settings()
        db_path = tmp_path / "paper-smoke.db"
        signal = build_signal()
        assessment = build_passing_assessment(signal.mint_address)

        assert assessment.all_checks_pass is True

        await init_db(db_path)

        adapter = SmokePaperExecutionAdapter()
        scorer = PassingRiskScorer(assessment)
        manager = PositionManager(db_path, settings)
        engine = DecisionEngine(adapter, scorer, manager, settings, db=db_path)

        trade = await engine.evaluate_signal(signal)

        assert trade is not None
        assert trade.mint_address == signal.mint_address
        assert trade.tx_signature is not None
        assert trade.tx_signature.startswith("PAPER-SMOKE-")
        assert 0 < trade.amount_sol <= settings.position.max_single_position_sol
        assert trade.amount_sol == expected_position_size(
            signal,
            settings.position.max_single_position_sol,
            settings.position.max_portfolio_sol,
        )
        assert trade.mode == "paper"
        assert len(scorer.calls) == 1
        assert len(adapter.executed) == 1

        recent_trades = load_recent_trades(db_path, limit=5)
        persisted_trade = next(item for item in recent_trades if item.mint_address == signal.mint_address)

        assert persisted_trade.tx_signature == trade.tx_signature
        assert persisted_trade.amount_sol == trade.amount_sol
        assert persisted_trade.metadata["strategy"] == "offline_e2e_smoke"

        open_positions = load_open_positions(db_path)
        position = next(item for item in open_positions if item.mint_address == signal.mint_address)

        assert position.status == PositionStatus.OPEN
        assert position.amount_sol == trade.amount_sol
        assert position.entry_trade_id == trade.id

        snapshot = load_dashboard_snapshot(settings, db_path=db_path)

        assert snapshot.recent_trades
        assert snapshot.open_positions
        assert snapshot.total_exposure_sol > 0
        assert any(item.mint_address == signal.mint_address for item in snapshot.recent_trades)
        assert any(item.mint_address == signal.mint_address for item in snapshot.open_positions)

        run_dashboard(db_path=db_path, once=True)
        await adapter.close()

    asyncio.run(run())
