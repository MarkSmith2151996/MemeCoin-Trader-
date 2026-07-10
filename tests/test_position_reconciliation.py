"""Tests for position reconciliation with mode-aware filtering."""

import asyncio
from pathlib import Path

from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import CheckResult, Position, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo, Trade
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


class LiveExecutionAdapter(SmokePaperExecutionAdapter):
    @property
    def mode(self):
        return "live"


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


async def _seed_paper_position(db_path: Path, mint_address: str = FAKE_MINT):
    """Seed a paper-mode open position via the full signal pipeline."""
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


async def _seed_live_position(db_path: Path, mint_address: str = FAKE_MINT):
    """Seed a live-mode open position via the full signal pipeline."""
    settings = load_settings()
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    engine = DecisionEngine(
        LiveExecutionAdapter(),
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


# ── Backward compatibility ──────────────────────────────────────────

def test_legacy_position_defaults_to_paper() -> None:
    """Positions created without a mode field default to 'paper'."""
    p = Position(mint_address="test", entry_trade_id="t1", amount_sol=1.0, token_amount=100.0, entry_price_sol=0.01)
    assert p.mode == "paper"


# ── Paper positions ignored by reconciliation ───────────────────────

def test_paper_local_only_position_is_not_flagged(tmp_path: Path) -> None:
    """Paper-mode local-only positions should NOT produce a mismatch."""
    async def run() -> None:
        manager, position = await _seed_paper_position(tmp_path / "paper-only.db")

        async def wallet_holdings():
            return {}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is True
        assert report.diagnostics == ("no_live_positions_to_reconcile",)
        assert report.mismatches == ()

    asyncio.run(run())


def test_no_live_positions_empty_db_passes(tmp_path: Path) -> None:
    """An empty DB with no positions at all passes reconciliation."""
    async def run() -> None:
        settings = load_settings()
        db_path = tmp_path / "empty.db"
        await init_db(db_path)
        manager = PositionManager(db_path, settings)

        async def wallet_holdings():
            return {}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is True
        assert report.diagnostics == ("no_live_positions_to_reconcile",)

    asyncio.run(run())


# ── Live positions included in reconciliation ───────────────────────

def test_live_local_only_position_is_flagged(tmp_path: Path) -> None:
    """Live-mode local-only positions SHOULD produce a mismatch."""
    async def run() -> None:
        manager, position = await _seed_live_position(tmp_path / "live-only.db")

        async def wallet_holdings():
            return {}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert report.mismatches[0].kind == "local_only_position"
        assert report.mismatches[0].mint_address == position.mint_address

    asyncio.run(run())


def test_live_and_wallet_match_passes(tmp_path: Path) -> None:
    """Live positions matching wallet holdings pass reconciliation."""
    async def run() -> None:
        manager, position = await _seed_live_position(tmp_path / "live-match.db")

        async def wallet_holdings():
            return {position.mint_address: position.token_amount}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is True
        assert report.diagnostics == ("position_reconciliation_passed",)
        assert report.mismatches == ()

    asyncio.run(run())


def test_live_balance_mismatch_is_flagged(tmp_path: Path) -> None:
    """Live positions with mismatched wallet balances are flagged."""
    async def run() -> None:
        manager, position = await _seed_live_position(tmp_path / "live-balance.db")

        async def wallet_holdings():
            return {position.mint_address: position.token_amount * 0.5}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert report.mismatches[0].kind == "balance_mismatch"

    asyncio.run(run())


# ── Mixed paper + live ──────────────────────────────────────────────

def test_mixed_paper_and_live_only_live_is_reconciled(tmp_path: Path) -> None:
    """When both paper and live positions exist, only live positions are reconciled."""
    async def run() -> None:
        settings = load_settings()
        db_path = tmp_path / "mixed.db"
        await init_db(db_path)
        manager = PositionManager(db_path, settings)

        # Seed a paper position
        paper_engine = DecisionEngine(
            SmokePaperExecutionAdapter(),
            PassingRiskScorer(_assessment("PaperOnly111111111111111111111111111111111")),
            manager,
            settings,
            db=db_path,
        )
        paper_signal = Signal(
            source=SignalSource.PUMP_FUN, type=SignalType.NEW_POOL,
            mint_address="PaperOnly111111111111111111111111111111111", confidence=0.8,
        )
        paper_trade = await paper_engine.evaluate_signal(paper_signal)
        assert paper_trade is not None

        # Seed a live position
        live_signal = Signal(
            source=SignalSource.PUMP_FUN, type=SignalType.NEW_POOL,
            mint_address="LiveOnly1111111111111111111111111111111111", confidence=0.8,
        )
        live_engine = DecisionEngine(
            LiveExecutionAdapter(),
            PassingRiskScorer(_assessment("LiveOnly1111111111111111111111111111111111")),
            manager,
            settings,
            db=db_path,
        )
        live_trade = await live_engine.evaluate_signal(live_signal)
        assert live_trade is not None

        all_positions = load_open_positions(db_path)
        assert len(all_positions) == 2

        # Wallet has neither — only the live position should cause a mismatch
        async def wallet_holdings():
            return {}

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        assert len(report.mismatches) == 1
        assert report.mismatches[0].kind == "local_only_position"
        assert report.mismatches[0].mint_address == "LiveOnly1111111111111111111111111111111111"

    asyncio.run(run())


def test_same_mint_paper_and_live_positions_coexist_by_mode(tmp_path: Path) -> None:
    async def run() -> None:
        settings = load_settings()
        db_path = tmp_path / "same-mint.db"
        await init_db(db_path)
        manager = PositionManager(db_path, settings)

        for mode, amount_sol in (("paper", 0.1), ("live", 0.2)):
            await manager.open_position(
                Trade(
                    mint_address=FAKE_MINT,
                    side="BUY",
                    amount_sol=amount_sol,
                    token_amount=amount_sol * 1000,
                    price_sol=0.001,
                    mode=mode,
                ),
                None,
            )

        reloaded = PositionManager(db_path, settings)
        paper = await reloaded.get_position(FAKE_MINT, mode="paper")
        live = await reloaded.get_position(FAKE_MINT, mode="live")

        assert paper is not None and paper.amount_sol == 0.1
        assert live is not None and live.amount_sol == 0.2
        assert len(await reloaded.get_all_open(mode="paper")) == 1
        assert len(await reloaded.get_all_open(mode="live")) == 1
        assert await reloaded.total_exposure_sol(mode="paper") == 0.1
        assert await reloaded.total_exposure_sol(mode="live") == 0.2

    asyncio.run(run())


# ── Edge cases ──────────────────────────────────────────────────────

def test_wallet_only_holding_is_flagged_with_live_positions(tmp_path: Path) -> None:
    """Wallet-only holdings are flagged when there are live positions."""
    async def run() -> None:
        manager, position = await _seed_live_position(tmp_path / "wallet-only-live.db")

        async def wallet_holdings():
            return {
                position.mint_address: position.token_amount,
                "wallet-only-mint": 123.0,
            }

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is False
        assert report.diagnostics == ("position_reconciliation_mismatch",)
        kinds = {m.kind for m in report.mismatches}
        assert "wallet_only_holding" in kinds

    asyncio.run(run())


def test_missing_wallet_data_fails_closed(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "missing-wallet.db"
        manager, _ = await _seed_live_position(db_path, FAKE_MINT)

        async def wallet_holdings():
            return None

        report = await reconcile_positions(manager, wallet_holdings)
        assert report.ok is False
        assert report.diagnostics == ("wallet_holdings_unknown",)

    asyncio.run(run())


def test_missing_lookup_fails_closed(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "missing-lookup.db"
        manager, _ = await _seed_live_position(db_path, FAKE_MINT)

        report = await reconcile_positions(manager, None)
        assert report.ok is False
        assert report.diagnostics == ("wallet_holdings_lookup_unavailable",)

    asyncio.run(run())


# ── Default mode ────────────────────────────────────────────────────

def test_default_mode_is_paper() -> None:
    """Default mode for new positions without explicit mode is 'paper'."""
    p = Position(mint_address="test", entry_trade_id="t1", amount_sol=1.0, token_amount=100.0, entry_price_sol=0.01)
    assert p.mode == "paper"
