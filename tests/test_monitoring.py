from datetime import UTC, datetime

from src.core.config import Settings
from src.core.models import Position, PositionStatus, Trade
from src.monitoring.alerts import AlertManager, format_alert_message
from src.monitoring.dashboard import SUPPORTED_HEALTH_INTERFACE, load_dashboard_snapshot, summarize_positions


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
    assert SUPPORTED_HEALTH_INTERFACE in snapshot.warnings


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
