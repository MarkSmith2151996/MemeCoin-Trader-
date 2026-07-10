"""Coverage: paper-state CLI command for inspecting/cleaning paper positions.

All tests use CLI invocation — no real trades or wallet access.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.database import init_db
from src.core.models import Position, Trade
from src.strategy.position_manager import PositionManager
from src.core.config import load_settings


runner = CliRunner()


def _paper_position(manager, mint: str) -> None:
    """Insert a paper-mode position directly."""
    import asyncio
    trade = Trade(
        mint_address=mint,
        side="BUY",
        amount_sol=1.0,
        token_amount=100000.0,
        price_sol=0.00001,
        mode="paper",
        status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))


def _live_position(manager, mint: str) -> None:
    """Insert a live-mode position directly."""
    import asyncio
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


def test_paper_state_lists_positions_read_only(tmp_path: Path) -> None:
    """Default paper-state lists positions without modifying them."""
    db = tmp_path / "state.db"
    result = runner.invoke(cli_module.app, ["paper-state", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Open paper positions" in result.stdout


def test_paper_state_with_cleanup_requires_confirm(tmp_path: Path) -> None:
    """Cleanup without --confirm prints warning and does nothing."""
    db = tmp_path / "no-clean.db"
    import asyncio
    asyncio.run(init_db(db))
    manager = PositionManager(db, load_settings())
    _paper_position(manager, "PaperMint11111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-state", "--cleanup", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Use --confirm" in result.stdout


def test_paper_state_cleanup_closes_paper_only(tmp_path: Path) -> None:
    """Cleanup with --confirm closes paper positions but not live."""
    db = tmp_path / "clean.db"
    import asyncio
    asyncio.run(init_db(db))
    manager = PositionManager(db, load_settings())
    _paper_position(manager, "PaperMint11111111111111111111111111111111111")
    _live_position(manager, "LiveMint111111111111111111111111111111111111")

    result = runner.invoke(cli_module.app, ["paper-state", "--cleanup", "--confirm", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Closed 1 paper position(s)" in result.stdout
    assert "1 live position(s) untouched" in result.stdout

    # Verify live position still open
    remaining = asyncio.run(manager.get_all_open())
    assert len(remaining) == 1
    assert remaining[0].mode == "live"


def test_paper_state_default_is_read_only(tmp_path: Path) -> None:
    """Default paper-state invocation is read-only — no positions are closed."""
    db = tmp_path / "readonly.db"
    import asyncio
    asyncio.run(init_db(db))
    manager = PositionManager(db, load_settings())
    _paper_position(manager, "PaperMint11111111111111111111111111111111111")

    # Read-only list
    result = runner.invoke(cli_module.app, ["paper-state", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "mint=PaperMint" in result.stdout

    # Position is still open
    positions = asyncio.run(manager.get_all_open())
    assert len(positions) == 1


def test_paper_state_does_not_print_secrets(tmp_path: Path) -> None:
    """Paper-state output contains no secret values."""
    db = tmp_path / "secrets.db"
    result = runner.invoke(cli_module.app, ["paper-state", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output
    assert "rpc_url=" not in output
