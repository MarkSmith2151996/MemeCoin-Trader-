import asyncio
from pathlib import Path

from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.execution.base import ExecutionAdapter
from src.execution.position_reconciliation import reconcile_positions
from src.monitoring.dashboard import load_open_positions
from src.strategy.decision_engine import DecisionEngine
from src.strategy.position_manager import PositionManager


FAKE_MINT = "ReconMint11111111111111111111111111111111111"


class SmokePaperExecutionAdapter(ExecutionAdapter):
    def __init__(self, price_sol: float = 0.00001) -> None:
        self.price_sol = price_sol

    async def execute_swap(self, mint_address, side, amount_sol, slippage_bps=300):
        from src.core.models import Trade

        return Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=amount_sol / self.price_sol,
            price_sol=self.price_sol,
            slippage_bps=slippage_bps,
            tx_signature="RECON-PAPER-1",
            mode=self.mode,
            status="simulated",
        )

    async def get_quote(self, mint_address, side, amount_sol, slippage_bps=300):
        raise NotImplementedError

    async def get_current_price(self, mint_address):
        return self.price_sol

    async def close(self):
        return None

    @property
    def mode(self):
        return "paper"


class PassingRiskScorer:
    def __init__(self, assessment: RiskAssessment) -> None:
        self.assessment = assessment

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        return self.assessment.model_copy(update={"token": self.assessment.token or TokenInfo(mint_address=signal.mint_address, liquidity_sol=100.0)})


def _assessment(mint_address: str) -> RiskAssessment:
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


async def _seed_open_position(db_path: Path, mint_address: str = FAKE_MINT):
    settings = load_settings()
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    engine = DecisionEngine(
        SmokePaperExecutionAdapter(),
        PassingRiskScorer(_assessment(mint_address)),
        manager,
        settings,
        db=db_path,
    )
    signal = Signal(source=SignalSource.PUMP_FUN, type=SignalType.NEW_POOL, mint_address=mint_address, confidence=0.8)
    trade = await engine.evaluate_signal(signal)
    assert trade is not None
    positions = load_open_positions(db_path)
    return manager, positions[0]


def test_clean_local_and_wallet_match_passes(tmp_path: Path) -> None:
    async def run() -> None:
        manager, position = await _seed_open_position(tmp_path / "recon-match.db")

        async def wallet_holdings():
            return {position.mint_address: position.token_amount}

        report = await reconcile_positions(manager, wallet_holdings)

        assert report.ok is True
        assert report.diagnostics == ("position_reconciliation_passed",)
        assert report.mismatches == ()

    asyncio.run(run())


def test_wallet_only_token_holding_is_flagged(tmp_path: Path) -> None:
    async def run() -> None:
        settings = load_settings()
        db_path = tmp_path / "wallet-only.db"
        await init_db(db_path)
        manager = PositionManager(db_path, settings)

        async def wallet_holdings():
            return {"wallet-only-mint": 123.0}

        report = await reconcile_positions(manager, wallet_holdings)

        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert report.mismatches[0].kind == "wallet_only_holding"

    asyncio.run(run())


def test_local_only_position_is_flagged(tmp_path: Path) -> None:
    async def run() -> None:
        manager, position = await _seed_open_position(tmp_path / "local-only.db")

        async def wallet_holdings():
            return {}

        report = await reconcile_positions(manager, wallet_holdings)

        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert report.mismatches[0].kind == "local_only_position"
        assert report.mismatches[0].mint_address == position.mint_address

    asyncio.run(run())


def test_balance_mismatch_is_flagged(tmp_path: Path) -> None:
    async def run() -> None:
        manager, position = await _seed_open_position(tmp_path / "balance-mismatch.db")

        async def wallet_holdings():
            return {position.mint_address: position.token_amount * 0.5}

        report = await reconcile_positions(manager, wallet_holdings)

        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert report.mismatches[0].kind == "balance_mismatch"

    asyncio.run(run())


def test_missing_wallet_data_fails_closed(tmp_path: Path) -> None:
    async def run() -> None:
        manager, _position = await _seed_open_position(tmp_path / "missing-wallet.db")

        async def wallet_holdings():
            return None

        report = await reconcile_positions(manager, wallet_holdings)

        assert report.ok is False
        assert report.diagnostics == ("wallet_holdings_unknown",)

    asyncio.run(run())


def test_missing_lookup_fails_closed(tmp_path: Path) -> None:
    async def run() -> None:
        manager, _position = await _seed_open_position(tmp_path / "missing-lookup.db")
        report = await reconcile_positions(manager, None)

        assert report.ok is False
        assert report.diagnostics == ("wallet_holdings_lookup_unavailable",)

    asyncio.run(run())
