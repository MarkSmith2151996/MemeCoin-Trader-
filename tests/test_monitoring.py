import asyncio
from datetime import UTC, datetime
from pathlib import Path

from src.core.config import Settings
from src.core.database import init_db
from src.core.models import Position, PositionStatus, Trade
from src.monitoring.alerts import AlertManager, format_alert_message
from src.monitoring.dashboard import load_dashboard_snapshot, resolve_db_path, run_dashboard, summarize_positions
from src.monitoring.health import HealthMonitor


def test_summarize_positions_uses_open_exposure_only() -> None:
    positions = [
        Position(
            mint_address="mint-open",
            entry_trade_id="trade-1",
            amount_sol=1.5,
            token_amount=1000,
            entry_price_sol=0.1,
            status=PositionStatus.OPEN,
        ),
        Position(
            mint_address="mint-closed",
            entry_trade_id="trade-2",
            amount_sol=0.25,
            token_amount=100,
            entry_price_sol=0.05,
            status=PositionStatus.CLOSED,
        ),
    ]

    summary = summarize_positions(positions)

    assert summary == {"open_count": 2, "open_exposure_sol": 1.5}


def test_load_dashboard_snapshot_handles_missing_db() -> None:
    snapshot = load_dashboard_snapshot(Settings(), db_path="/tmp/definitely-missing-memecoin.db")

    assert snapshot.recent_trades == []
    assert snapshot.recent_signals == []
    assert snapshot.open_positions == []
    assert any("Database not found" in warning for warning in snapshot.warnings)


def test_health_monitor_matches_dashboard_contract() -> None:
    status = HealthMonitor(max_staleness_s=120).status()

    assert status["monitoring"]["ok"] is True
    assert status["monitoring"]["message"] == "memecoin-trader scaffold healthy"
    assert HealthMonitor(max_staleness_s=120).stale_components() == []


def test_resolve_db_path_uses_project_default(monkeypatch) -> None:
    monkeypatch.delenv("MEMECOIN_DB_PATH", raising=False)
    monkeypatch.delenv("DATABASE_PATH", raising=False)

    assert resolve_db_path() == Path("data/trades.db")


def test_run_dashboard_once_renders_without_signal_warning(tmp_path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))

    run_dashboard(db_path=db_path, once=True)

    snapshot = load_dashboard_snapshot(Settings(), db_path=db_path)
    assert "No persisted signals available" not in snapshot.warnings


def test_alert_manager_defaults_to_log_channel_only() -> None:
    manager = AlertManager()

    assert manager.enabled_channels() == ["log"]


def test_alert_manager_formats_trade_message() -> None:
    trade = Trade(
        mint_address="So11111111111111111111111111111111111111112",
        amount_sol=0.5,
        mode="paper",
        status="simulated",
        side="BUY",
        executed_at=datetime.now(UTC),
    )

    message = format_alert_message(
        {
            "level": "info",
            "title": "Trade executed",
            "message": f"{trade.side.value} {trade.amount_sol:.4f} SOL on {trade.mint_address} in {trade.mode} mode",
        }
    )

    assert message.startswith("[INFO] Trade executed")
    assert trade.mint_address in message
