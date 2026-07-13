"""Focused coverage for the explicit paper-only migration quote lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite

import src.cli as cli_module
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType
from src.execution.price_provider import PriceProvider, PriceResult
from src.signals.base import SignalSource as SignalSourceBase


class FakeMigrationSource(SignalSourceBase):
    def __init__(self, batches: list[list[Signal]]) -> None:
        self._batches = list(batches)
        self.started = False
        self.stopped = False

    @property
    def name(self) -> str:
        return "fake_migration"

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def poll(self) -> list[Signal]:
        return self._batches.pop(0) if self._batches else []


class SequencedPriceProvider(PriceProvider):
    def __init__(self, prices: list[float | None]) -> None:
        self._prices = iter(prices)

    @property
    def name(self) -> str:
        return "fake_dexscreener"

    async def get_current_price(self, mint_address: str) -> float | None:
        return (await self.get_price_with_diagnostic(mint_address)).price_sol

    async def get_price_with_diagnostic(self, mint_address: str) -> PriceResult:
        price = next(self._prices)
        return PriceResult(
            price_sol=price,
            reason="live_dexscreener" if price is not None else "no_requested_mint_sol_pair",
            liquidity_usd=50_000.0 if price is not None else None,
        )


def _migration_signal(mint: str = "MigrationLifecycleMint111111111111111111111") -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.GRADUATION,
        mint_address=mint,
        confidence=0.95,
        payload={"txType": "migrate", "pool": "raydium", "symbol": "MIG"},
    )


def _launch_signal() -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="LaunchLifecycleMint11111111111111111111111111",
    )


async def _eligible_assessment(_signal: Signal) -> RiskAssessment:
    return RiskAssessment(
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.UNKNOWN,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.UNKNOWN,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.UNKNOWN,
    )


async def _counts(db_path: Path) -> tuple[int, int, int]:
    async with aiosqlite.connect(db_path) as db:
        trades = (await (await db.execute("SELECT COUNT(*) FROM trades")).fetchone())[0]
        open_positions = (
            await (await db.execute("SELECT COUNT(*) FROM positions WHERE status != 'CLOSED'")).fetchone()
        )[0]
        closed_positions = (
            await (await db.execute("SELECT COUNT(*) FROM positions WHERE status = 'CLOSED'")).fetchone()
        )[0]
    return int(trades), int(open_positions), int(closed_positions)


def test_confirmed_migration_lifecycle_enters_marks_and_closes_one_quoted_position(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "migration-lifecycle.db"
        source = FakeMigrationSource([[_launch_signal(), _migration_signal()]])
        summary = await cli_module.run_migration_paper_lifecycle(
            max_candidates=1,
            timeout_seconds=0.1,
            amount_sol=0.01,
            confirm=True,
            db_path=db_path,
            source=source,
            assessor=_eligible_assessment,
            price_provider=SequencedPriceProvider([0.00001, 0.000012]),
        )

        assert source.started is True
        assert source.stopped is True
        assert summary.execution_mode == "paper"
        assert summary.candidates_seen == 2
        assert summary.migration_candidates == 1
        assert summary.paper_minimum_eligible == 1
        assert summary.quote_available == 1
        assert summary.outcome == "closed"
        assert summary.entry_price_sol == 0.00001
        assert summary.mark_price_sol == 0.000012
        assert summary.realized_pnl_sol == 0.002
        assert await _counts(db_path) == (1, 0, 1)

        report_path = tmp_path / "review.md"
        cli_module.write_migration_paper_lifecycle_report(summary, report_path)
        report = report_path.read_text(encoding="utf-8")
        assert "outcome=closed" in report
        assert "paper-only" in report
        assert "raw_data" not in report

    asyncio.run(run())


def test_migration_lifecycle_requires_confirmation_before_persisting_entry(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "confirmation-required.db"
        summary = await cli_module.run_migration_paper_lifecycle(
            timeout_seconds=0.1,
            db_path=db_path,
            source=FakeMigrationSource([[_migration_signal()]]),
            assessor=_eligible_assessment,
            price_provider=SequencedPriceProvider([0.00001]),
        )

        assert summary.outcome == "quote_ready_confirmation_required"
        assert summary.entry_price_sol == 0.00001
        assert await _counts(db_path) == (0, 0, 0)

    asyncio.run(run())


def test_migration_lifecycle_never_enters_without_requested_mint_wsol_quote(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "quote-required.db"
        summary = await cli_module.run_migration_paper_lifecycle(
            timeout_seconds=0.1,
            confirm=True,
            db_path=db_path,
            source=FakeMigrationSource([[_migration_signal()]]),
            assessor=_eligible_assessment,
            price_provider=SequencedPriceProvider([None]),
        )

        assert summary.outcome == "no_entry"
        assert summary.reason == "no_requested_mint_sol_pair"
        assert await _counts(db_path) == (0, 0, 0)

    asyncio.run(run())


def test_migration_lifecycle_keeps_quoted_entry_open_when_post_entry_mark_is_unavailable(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "mark-unavailable.db"
        summary = await cli_module.run_migration_paper_lifecycle(
            timeout_seconds=0.1,
            confirm=True,
            db_path=db_path,
            source=FakeMigrationSource([[_migration_signal()]]),
            assessor=_eligible_assessment,
            price_provider=SequencedPriceProvider([0.00001, None]),
        )

        assert summary.outcome == "mark_unavailable_position_open"
        assert summary.reason == "no_requested_mint_sol_pair"
        assert await _counts(db_path) == (1, 1, 0)

    asyncio.run(run())
