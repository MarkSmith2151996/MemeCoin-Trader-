"""Coverage: paper PnL mark-to-market and paper-close CLI commands.

All tests use CLI invocation with temporary DBs — no real trades or wallet access.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.core.database import get_recent_soak_runs, init_db, record_soak_run
from src.core.models import Position, PositionStatus, Signal, SignalSource, SignalType, SoakRunRecord, Trade
from src.execution.paper_pnl import PaperPnLCalculator
from src.execution.price_provider import FakePriceProvider, UnavailablePriceProvider
from src.strategy.position_manager import PositionManager


runner = CliRunner()


def _paper_position(manager: PositionManager, mint: str, amount_sol: float = 1.0, price_sol: float = 0.00001) -> None:
    trade = Trade(
        mint_address=mint,
        side="BUY",
        amount_sol=amount_sol,
        token_amount=amount_sol / price_sol,
        price_sol=price_sol,
        mode="paper",
        status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))


def _live_position(manager: PositionManager, mint: str) -> None:
    trade = Trade(
        mint_address=mint,
        side="BUY",
        amount_sol=1.0,
        token_amount=100000.0,
        price_sol=0.00001,
        mode="live",
        status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))


# --- MT-123: paper fill modeling improvements ---

def test_valid_paper_fill_produces_meaningful_entry(tmp_path: Path) -> None:
    db = tmp_path / "valid_fill.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)

    trade = Trade(
        mint_address="ValidFill1111111111111111111111111111111111",
        side="BUY",
        amount_sol=1.0,
        token_amount=100000.0,
        price_sol=0.00001,
        mode="paper",
        status="simulated",
    )
    position = asyncio.run(manager.open_position(trade, None))

    assert position.token_amount == 100000.0
    assert position.entry_price_sol == 0.00001
    assert position.amount_sol == 1.0

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.open_positions == 1
    assert summary.total_sol_deployed == 1.0
    assert summary.positions[0].token_amount == 100000.0
    assert summary.positions[0].entry_price_sol == 0.00001


def test_missing_price_does_not_invent_price(tmp_path: Path) -> None:
    db = tmp_path / "no_price_fill.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)

    trade = Trade(
        mint_address="NoPriceMint22222222222222222222222222222222",
        side="BUY",
        amount_sol=1.0,
        token_amount=None,
        price_sol=None,
        mode="paper",
        status="simulated",
    )
    position = asyncio.run(manager.open_position(trade, None))

    assert position.token_amount == 0.0
    assert position.entry_price_sol == 0.0
    assert position.amount_sol == 1.0

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.open_positions == 1
    assert summary.mark_unavailable_count == 1
    assert summary.unrealized_pnl_sol is None
    pos = summary.positions[0]
    assert pos.mark_price_sol is None
    assert pos.mark_unavailable is True
    assert pos.unrealized_pnl_sol is None


def test_legacy_zero_token_readable_as_na(tmp_path: Path) -> None:
    db = tmp_path / "legacy_zero.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)

    position = Position(
        mint_address="LegacyZero11111111111111111111111111111111",
        entry_trade_id="legacy-trade-1",
        amount_sol=1.0,
        token_amount=0.0,
        entry_price_sol=1.0,
        mode="paper",
    )
    from src.core.database import record_position
    asyncio.run(record_position(db, position))
    manager._cache[position.mint_address] = position

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.open_positions == 1
    assert summary.unrealized_pnl_sol is None
    assert summary.mark_unavailable_count == 1

    pos = summary.positions[0]
    assert pos.token_amount == 0.0
    assert pos.mark_unavailable is True
    assert pos.unrealized_pnl_sol is None


def test_paper_fill_rejects_invalid_price(tmp_path: Path) -> None:
    db = tmp_path / "reject_invalid.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)

    import math

    trade = Trade(
        mint_address="InvalidZeroPrice11111111111111111111111111",
        side="BUY",
        amount_sol=1.0,
        token_amount=None,
        price_sol=0.0,
        mode="paper",
        status="simulated",
    )
    raised = False
    try:
        asyncio.run(manager.open_position(trade, None))
    except ValueError:
        raised = True
    assert raised, "Expected ValueError for price_sol=0.0 via trade"


# Requirement 1: paper-pnl reports exposure but no PnL when no marks/exits exist
def test_paper_pnl_reports_exposure_without_marks(tmp_path: Path) -> None:
    db = tmp_path / "no_marks.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PaperMint11111111111111111111111111111111111", amount_sol=2.0, price_sol=0.00002)

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.open_positions == 1
    assert summary.closed_positions == 0
    assert summary.total_sol_deployed == 2.0
    assert summary.realized_pnl_sol == 0.0
    assert summary.unrealized_pnl_sol is None
    assert summary.mark_unavailable_count == 1
    assert summary.unrealized_incomplete is True


# Requirement 2: open paper positions with fake mark prices produce unrealized PnL
def test_paper_pnl_shows_unrealized_pnl_with_marks(tmp_path: Path) -> None:
    db = tmp_path / "with_marks.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ProfitableMint1111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    fake_prices = FakePriceProvider({"ProfitableMint1111111111111111111111111111111": 0.00002})
    calculator = PaperPnLCalculator(manager, price_provider=fake_prices)
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.open_positions == 1
    assert summary.realized_pnl_sol == 0.0
    assert summary.unrealized_pnl_sol is not None
    assert summary.unrealized_incomplete is False
    assert summary.mark_unavailable_count == 0
    # token_amount = 1.0 / 0.00001 = 100000, unrealized = 100000 * 0.00002 - 1.0 = 1.0
    assert summary.unrealized_pnl_sol == 1.0

    pos_detail = summary.positions[0]
    assert pos_detail.mark_price_sol == 0.00002
    assert pos_detail.unrealized_pnl_sol == 1.0
    assert pos_detail.unrealized_pnl_pct == 100.0
    assert pos_detail.mark_unavailable is False


# Requirement 3: closed paper positions with exit prices produce realized PnL
def test_paper_pnl_shows_realized_pnl_for_closed(tmp_path: Path) -> None:
    db = tmp_path / "realized.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ClosedMint1111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    asyncio.run(manager.close_position("ClosedMint1111111111111111111111111111111111", exit_price_sol=0.00002))

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.total_positions == 1
    assert summary.closed_positions == 1
    assert summary.open_positions == 0
    # token_amount = 1.0 / 0.00001 = 100000, realized = 100000 * 0.00002 - 1.0 = 1.0
    assert summary.realized_pnl_sol == 1.0


# Requirement 4: closing a paper position by mint records exit and realized PnL
def test_paper_close_records_exit_and_pnl(tmp_path: Path) -> None:
    db = tmp_path / "close_pnl.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "CloseMeMint111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    closed = asyncio.run(manager.close_position("CloseMeMint111111111111111111111111111111111", exit_price_sol=0.00003))
    assert closed is not None
    assert closed.status == PositionStatus.CLOSED
    assert closed.close_price_sol == 0.00003
    assert closed.closed_at is not None
    # token_amount = 1.0 / 0.00001 = 100000, realized = 100000 * 0.00003 - 1.0 = 2.0
    assert closed.realized_pnl_sol == 2.0


# Requirement 5: close-all requires explicit confirmation
def test_paper_close_all_requires_confirm(tmp_path: Path) -> None:
    db = tmp_path / "close_all_confirm.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PaperA1111111111111111111111111111111111111")
    _paper_position(manager, "PaperB1111111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-close", "--all", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Use --confirm" in result.stdout

    positions = asyncio.run(manager.get_all_open())
    assert len(positions) == 2


def test_paper_close_all_with_confirm_closes_paper(tmp_path: Path) -> None:
    db = tmp_path / "close_all_works.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PaperA1111111111111111111111111111111111111")
    _paper_position(manager, "PaperB1111111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-close", "--all", "--confirm", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Closed 2 paper position(s)" in result.stdout
    assert "simulated" in result.stdout.lower()

    positions = asyncio.run(manager.get_all_open())
    assert len(positions) == 0


# Requirement 6: close commands never touch mode=="live" positions
def test_paper_close_all_never_touches_live(tmp_path: Path) -> None:
    db = tmp_path / "close_live_protected.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PaperMint11111111111111111111111111111111111")
    _live_position(manager, "LiveMint111111111111111111111111111111111111")

    asyncio.run(manager.close_position("PaperMint11111111111111111111111111111111111", exit_price_sol=0.00002))

    remaining = asyncio.run(manager.get_all_open())
    assert len(remaining) == 1
    assert remaining[0].mode == "live"


def test_paper_close_rejects_live_position(tmp_path: Path) -> None:
    db = tmp_path / "reject_live_close.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _live_position(manager, "LiveMint111111111111111111111111111111111111")

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "LiveMint111111111111111111111111111111111111", "--db-path", str(db)],
    )
    assert result.exit_code != 0
    assert "Refusing to close a live position" in result.stdout


# Requirement 7: price unavailable does not invent PnL
def test_paper_pnl_no_invented_pnl(tmp_path: Path) -> None:
    db = tmp_path / "no_invent.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "NoPriceMint1111111111111111111111111111111111")

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.unrealized_pnl_sol is None
    assert summary.mark_unavailable_count == 1
    for pos in summary.positions:
        if pos.status == PositionStatus.OPEN:
            assert pos.mark_price_sol is None
            assert pos.mark_unavailable is True
            assert pos.unrealized_pnl_sol is None


# Requirement 8: no private key is required
def test_paper_pnl_no_private_key_required(tmp_path: Path) -> None:
    import os
    assert "TRADING_WALLET_PRIVATE_KEY" not in os.environ or os.environ["TRADING_WALLET_PRIVATE_KEY"] == ""

    db = tmp_path / "no_key.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "NoKeyMint111111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-pnl", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Paper PnL Summary" in result.stdout


# Requirement 9: no secrets are printed
def test_paper_pnl_no_secrets_printed(tmp_path: Path) -> None:
    db = tmp_path / "secrets_pnl.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "SecretMint11111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-pnl", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output
    assert "rpc_url=" not in output


def test_paper_close_no_secrets_printed(tmp_path: Path) -> None:
    db = tmp_path / "close_secrets.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "CloseSecret111111111111111111111111111111111")

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "CloseSecret111111111111111111111111111111111", "--price", "0.00002", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output


# Requirement 10: full existing paper-soak flow still passes
def test_paper_soak_still_passes(tmp_path: Path) -> None:
    db = tmp_path / "soak_stable.db"
    asyncio.run(init_db(db))

    from src.signals.base import SignalSource
    from src.core.models import Signal, SignalType, SignalSource as SignalSourceEnum

    class FakeSource(SignalSource):
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        @property
        def name(self) -> str:
            return "test_source"

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def poll(self) -> list[Signal]:
            return []

    settings = load_settings()
    source = FakeSource()

    summary = asyncio.run(
        cli_module.run_bounded_paper_cycle(
            max_signals=10,
            timeout_seconds=0.1,
            db_path=db,
            sources=[source],
            poll_interval_s=0.0,
        )
    )

    assert summary.signals_collected == 0
    assert summary.termination_reason == "timeout"
    assert source.started is True
    assert source.stopped is True


# CLI format tests
def test_paper_pnl_cli_shows_warning(tmp_path: Path) -> None:
    db = tmp_path / "warning.db"
    result = runner.invoke(cli_module.app, ["paper-pnl", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "simulated" in result.stdout.lower()
    assert "WARNING" in result.stdout


def test_paper_close_by_mint_with_price(tmp_path: Path) -> None:
    db = tmp_path / "close_by_mint.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "CloseByMint1111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "CloseByMint1111111111111111111111111111111111", "--price", "0.00002", "--db-path", str(db)],
    )
    assert result.exit_code == 0, f"stdout: {result.stdout}"
    assert "Closed paper position" in result.stdout
    assert "+1.000000 SOL" in result.stdout
    assert "(manual)" in result.stdout
    assert "simulated" in result.stdout.lower()

    state_result = runner.invoke(cli_module.app, ["paper-state", "--db-path", str(db)])
    assert "Open paper positions: 0" in state_result.stdout
    assert "Total open positions: 0" in state_result.stdout


def test_paper_close_requires_mint_or_all(tmp_path: Path) -> None:
    db = tmp_path / "require_arg.db"
    result = runner.invoke(cli_module.app, ["paper-close", "--db-path", str(db)])
    assert result.exit_code != 0
    assert "Provide --mint" in result.stdout


def test_paper_close_position_not_found(tmp_path: Path) -> None:
    db = tmp_path / "not_found.db"
    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "NonexistentMint1111111111111111111111111111", "--db-path", str(db)],
    )
    assert result.exit_code != 0
    assert "Position not found" in result.stdout


# --- MT-121: paper-close preview and safeguards ---

def test_paper_close_preview_does_not_mutate(tmp_path: Path) -> None:
    db = tmp_path / "preview_no_mutate.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PreviewMint111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "PreviewMint111111111111111111111111111111111", "--price", "0.00002", "--preview", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    assert "Preview only" in result.stdout
    assert "Estimated realized PnL" in result.stdout
    assert "(manual)" in result.stdout
    assert "simulated" in result.stdout.lower()

    positions = asyncio.run(manager.get_all_open())
    assert len(positions) == 1
    position = positions[0]
    assert position.status != "CLOSED"


def test_paper_close_preview_all_does_not_mutate(tmp_path: Path) -> None:
    db = tmp_path / "preview_all_no_mutate.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PreviewA11111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)
    _paper_position(manager, "PreviewB11111111111111111111111111111111111", amount_sol=2.0, price_sol=0.00002)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--all", "--price", "0.00003", "--preview", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    assert "Preview only" in result.stdout
    assert "Estimated total realized PnL" in result.stdout

    positions = asyncio.run(manager.get_all_open())
    assert len(positions) == 2


def test_paper_close_refuses_without_price(tmp_path: Path) -> None:
    db = tmp_path / "no_price_refuse.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "NoPriceClose1111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "NoPriceClose1111111111111111111111111111111", "--db-path", str(db)],
    )
    assert result.exit_code != 0
    assert "No exit price available" in result.stdout
    assert "Provide --price" in result.stdout
    assert "--use-mark" in result.stdout


def test_paper_close_shows_price_source(tmp_path: Path) -> None:
    db = tmp_path / "price_source.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "SourceMint1111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "SourceMint1111111111111111111111111111111111", "--price", "0.00002", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    assert "(manual)" in result.stdout
    assert "simulated" in result.stdout.lower()


def test_paper_close_preview_shows_price_source(tmp_path: Path) -> None:
    db = tmp_path / "preview_source.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PrevSrcMint111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "PrevSrcMint111111111111111111111111111111111", "--price", "0.00003", "--preview", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    assert "(manual)" in result.stdout
    assert "Preview only" in result.stdout
    assert "Estimated realized PnL" in result.stdout
    assert "+2.000000 SOL" in result.stdout


def test_paper_close_no_secrets_in_preview(tmp_path: Path) -> None:
    db = tmp_path / "preview_secrets.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "SecPreview11111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "SecPreview11111111111111111111111111111111", "--price", "0.00002", "--preview", "--db-path", str(db)],
    )
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output


# --- MT-122: paper-report daily trading report ---

def test_paper_report_empty_db(tmp_path: Path) -> None:
    db = tmp_path / "report_empty.db"
    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Paper Trading Report" in result.stdout
    assert "simulated" in result.stdout.lower()
    assert "Total paper trades entered: 0" in result.stdout
    assert "Open paper positions: 0" in result.stdout
    assert "Closed paper positions: 0" in result.stdout
    assert "no closed trades yet" in result.stdout
    assert "no open paper positions" in result.stdout


def test_paper_report_with_open_positions_no_marks(tmp_path: Path) -> None:
    db = tmp_path / "report_open.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ReportOpen1111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Paper Trading Report" in result.stdout
    assert "Open paper positions: 1" in result.stdout
    assert "mark_unavailable" in result.stdout
    assert "ReportOpen" in result.stdout


def test_paper_report_with_fake_marks(tmp_path: Path) -> None:
    db = tmp_path / "report_marks.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ReportMark1111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(cli_module.app, ["paper-report", "--marks", "live", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Marks: live" in result.stdout or "Marks: [green]live" in result.stdout
    assert "Paper Trading Report" in result.stdout


def test_paper_report_with_realized_pnl(tmp_path: Path) -> None:
    db = tmp_path / "report_realized.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ReportReal11111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)
    asyncio.run(manager.close_position("ReportReal11111111111111111111111111111111111", exit_price_sol=0.00002))

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Closed paper positions: 1" in result.stdout
    assert "Best closed trade" in result.stdout
    assert "+1.000000" in result.stdout


def test_paper_report_live_positions_untouched(tmp_path: Path) -> None:
    db = tmp_path / "report_live.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "PaperRep111111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    trade = Trade(
        mint_address="LiveReportMint111111111111111111111111111111",
        side="BUY", amount_sol=1.0, token_amount=100000.0,
        price_sol=0.00001, mode="live", status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Live positions (untouched): 1" in result.stdout
    assert "Open paper positions: 1" in result.stdout
    assert "total paper trades entered" in result.stdout.lower()


def test_paper_report_no_private_key_required(tmp_path: Path) -> None:
    import os
    assert "TRADING_WALLET_PRIVATE_KEY" not in os.environ or os.environ["TRADING_WALLET_PRIVATE_KEY"] == ""
    db = tmp_path / "report_no_key.db"
    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Paper Trading Report" in result.stdout


def test_paper_report_no_secrets_printed(tmp_path: Path) -> None:
    db = tmp_path / "report_secrets.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "ReportSec11111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output
    assert "rpc_url=" not in output


def test_paper_report_default_no_network(tmp_path: Path) -> None:
    db = tmp_path / "report_no_net.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "NoNetMint11111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Marks: unavailable" in result.stdout or "Marks:" in result.stdout
    assert "mark_unavailable" in result.stdout


# --- MT-124: paper-soak diagnostics persistence ---

def test_soak_run_db_table_created(tmp_path: Path) -> None:
    db = tmp_path / "soak_schema.db"
    asyncio.run(init_db(db))
    async def check():
        async with aiosqlite.connect(db) as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='paper_soak_runs'"
            )
            row = await cursor.fetchone()
            assert row is not None, "paper_soak_runs table not created"
    asyncio.run(check())


def test_soak_run_persist_and_read(tmp_path: Path) -> None:
    db = tmp_path / "soak_rw.db"
    asyncio.run(init_db(db))

    record = SoakRunRecord(
        max_signals=20,
        timeout_seconds=30.0,
        execution_mode="paper",
        risk_profile="discovery",
        signals_collected=15,
        signals_accepted=3,
        signals_rejected=12,
        trades_persisted=2,
        open_positions=2,
        source_failures_json='{"pump_fun": 1}',
        rejection_reasons_json='{"honeypot_check": 5}',
        capacity_blocked=3,
        unknown_data_blocks=1,
        unexpected_failures=0,
        termination_reason="max_signals",
        elapsed_seconds=12.5,
        health_ok=True,
        health_message="ok",
        guardrail_diagnostics_json='["paper_mode_unaffected"]',
        circuit_breaker_diagnostics_json='["paper_mode_unaffected"]',
        readiness_json='[]',
    )
    asyncio.run(record_soak_run(db, record))

    runs = asyncio.run(get_recent_soak_runs(db, limit=5))
    assert len(runs) == 1
    loaded = runs[0]
    assert loaded.signals_collected == 15
    assert loaded.signals_accepted == 3
    assert loaded.trades_persisted == 2
    assert loaded.capacity_blocked == 3
    assert loaded.unknown_data_blocks == 1
    assert loaded.unexpected_failures == 0
    assert loaded.termination_reason == "max_signals"
    assert loaded.health_ok is True


def test_soak_run_multi_run_ordering(tmp_path: Path) -> None:
    import uuid

    db = tmp_path / "soak_order.db"
    asyncio.run(init_db(db))

    for i in range(3):
        record = SoakRunRecord(
            id=str(uuid.uuid4()),
            max_signals=10,
            timeout_seconds=10.0,
            signals_collected=i * 10,
            signals_accepted=i,
            trades_persisted=i,
            source_failures_json="{}",
            rejection_reasons_json="{}",
            termination_reason="max_signals",
            elapsed_seconds=float(i),
        )
        asyncio.run(record_soak_run(db, record))

    runs = asyncio.run(get_recent_soak_runs(db, limit=2))
    assert len(runs) == 2
    assert runs[0].signals_collected >= runs[1].signals_collected


def test_paper_report_shows_soak_diagnostics(tmp_path: Path) -> None:
    db = tmp_path / "report_soak.db"
    asyncio.run(init_db(db))

    record = SoakRunRecord(
        max_signals=50,
        timeout_seconds=60.0,
        signals_collected=30,
        signals_accepted=5,
        signals_rejected=25,
        trades_persisted=3,
        open_positions=3,
        source_failures_json='{"pump_fun": 2}',
        rejection_reasons_json='{"honeypot_check": 10}',
        capacity_blocked=5,
        unknown_data_blocks=2,
        unexpected_failures=0,
        termination_reason="max_signals",
        elapsed_seconds=45.0,
        health_ok=True,
        health_message="ok",
        guardrail_diagnostics_json='["paper_mode_unaffected"]',
        circuit_breaker_diagnostics_json='["paper_mode_unaffected"]',
        readiness_json='[]',
    )
    asyncio.run(record_soak_run(db, record))

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Signals: 30 collected" in result.stdout
    assert "Trades entered: 3" in result.stdout
    assert "Capacity blocks: 5" in result.stdout
    assert "Source failures: pump_fun=2" in result.stdout


def test_paper_report_no_soak_data_shows_hint(tmp_path: Path) -> None:
    db = tmp_path / "no_soak_data.db"
    asyncio.run(init_db(db))

    result = runner.invoke(cli_module.app, ["paper-report", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "run 'paper-soak" in result.stdout
