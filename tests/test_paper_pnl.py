"""Coverage: paper PnL mark-to-market and paper-close CLI commands.

All tests use CLI invocation with temporary DBs — no real trades or wallet access.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import Position, PositionStatus, Trade
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
    assert "Realized PnL" in result.stdout
    assert "+1.000000 SOL" in result.stdout

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
