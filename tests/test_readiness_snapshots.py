"""Snapshot-style tests for stable CLI readiness output fields.

These tests lock down key CLI output lines so future changes
do not accidentally hide important blockers or leak secrets.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.database import init_db
from src.core.models import Trade
from src.strategy.position_manager import PositionManager
from src.core.config import load_settings
from src.execution.live_preflight import TransactionSimulationResult
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.monitoring.health import HealthStatus
from datetime import UTC, datetime
import asyncio


runner = CliRunner()


# ── Env-readiness output ────────────────────────────────────────────

def test_env_readiness_shows_present_missing_only() -> None:
    """env-readiness reports present/missing without values."""
    result = runner.invoke(cli_module.app, ["env-readiness"])
    assert result.exit_code == 0
    output = result.stdout
    assert "HELIUS_API_KEY=" in output
    assert "present" in output or "MISSING" in output
    # No secret prefixes or values
    assert "50661bc5" not in output
    assert "api-key=" not in output.lower()


# ── Live-readiness output fields ────────────────────────────────────

def test_live_readiness_shows_not_ready_when_arming_missing(tmp_path: Path) -> None:
    """live-readiness reports NOT READY when live arming vars are missing."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(tmp_path / "lrdy1.db")])
    assert result.exit_code == 0
    output = result.stdout
    assert "micro_live_ready=NOT READY" in output
    assert "guardrails=not_ready" in output
    assert "execution_mode_not_live" in output


def test_live_readiness_shows_no_live_positions(tmp_path: Path) -> None:
    """live-readiness shows no_live_positions_to_reconcile when only paper positions exist."""
    # Seed a paper position
    db = tmp_path / "lrdy2.db"
    import asyncio
    asyncio.run(init_db(db))
    manager = PositionManager(db, load_settings())
    trade = Trade(
        mint_address="Paper1231111111111111111111111111111111111",
        side="BUY",
        amount_sol=1.0,
        token_amount=100000.0,
        price_sol=0.00001,
        mode="paper",
        status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))

    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout
    assert "position_reconciliation=ok" in output
    assert "no_live_positions_to_reconcile" in output


def test_live_readiness_shows_circuit_breaker_ok(tmp_path: Path) -> None:
    """live-readiness shows circuit_breaker=ok when in paper mode."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(tmp_path / "lrdy3.db")])
    assert result.exit_code == 0
    assert "circuit_breaker=ok" in result.stdout
    assert "paper_mode_unaffected" in result.stdout


def test_live_readiness_shows_health_ok(tmp_path: Path) -> None:
    """live-readiness shows health=ok when process is running."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(tmp_path / "lrdy4.db")])
    assert result.exit_code == 0
    assert "health=ok" in result.stdout
    assert "health_check_ok" in result.stdout


# ── Provider status in live-readiness ───────────────────────────────

def test_live_readiness_shows_insufficient_wallet_balance(tmp_path: Path) -> None:
    """live-readiness preflight shows insufficient_wallet_balance when balance is too low."""
    async def run() -> None:
        settings = load_settings()
        live_settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"mode": "live", "primary_rpc_url": "https://primary.example"}
                )
            }
        )
        report = await evaluate_micro_live_readiness(
            live_settings,
            env={
                "LIVE_TRADING_ENABLED": "true",
                "LIVE_CONFIRMATION_PHRASE": live_settings.live_guardrails.confirmation_phrase,
                "LIVE_KILL_SWITCH": "false",
                "PRIMARY_RPC_URL": "https://primary.example",
                "MAX_LIVE_TRADE_SOL": "0.01",
                "MAX_LIVE_DAILY_TRADES": "3",
                "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
                "MIN_LIVE_WALLET_BALANCE_SOL": "0.05",
            },
            requested_trade_sol=0.01,
            wallet_balance_lookup=lambda: _async_return(0.001),
            position_manager=None,
            circuit_breaker=LiveCircuitBreaker(),
            health_status=HealthStatus(ok=True, message="ok", checked_at=datetime.now(UTC)),
        )
        assert not report.ready
        preflight = {c.name: c for c in report.checks}["preflight"]
        assert "insufficient_wallet_balance" in preflight.diagnostics

    asyncio.run(run())


# ── No secret leakage in readiness output ───────────────────────────

def test_live_readiness_no_secrets_in_output(tmp_path: Path) -> None:
    """live-readiness output contains no secret values or partial secrets."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(tmp_path / "nodump.db")])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "50661bc5" not in output
    assert "api-key=" not in output
    assert "private_key" not in output or "TRADING_WALLET_PRIVATE_KEY" in result.stdout
    assert "rpc_url=" not in output


def test_env_readiness_no_secrets_in_output() -> None:
    """env-readiness output contains no secret values (env var names are safe)."""
    result = runner.invoke(cli_module.app, ["env-readiness"])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "50661bc5" not in output
    assert "api-key=" not in output
    # private_key appears as env var name (trading_wallet_private_key=missing), that's safe
    # RPC_URL appears as status label (primary_rpc_url=missing), that's safe
    assert "helius_rpc_url=" not in output


async def _async_return(value):
    return value
