"""Terminal monitoring dashboard for memecoin-trader."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.core.config import Settings, load_settings
from src.core.models import Position, PositionStatus, Signal, SignalSource, SignalType, Trade
from src.monitoring import health as health_module
from src.strategy.portfolio import open_exposure_sol

DEFAULT_DB_PATH = Path("data/memecoin_trader.db")
SUPPORTED_HEALTH_INTERFACE = (
    "HealthMonitor(max_staleness_s=...).status()/stale_components() is unavailable in "
    "src.monitoring.health"
)


@dataclass(slots=True)
class DashboardSnapshot:
    """Current dashboard state rendered into the TUI."""

    captured_at: datetime
    execution_mode: str
    db_path: Path
    component_health: dict[str, dict[str, Any]] = field(default_factory=dict)
    stale_components: list[str] = field(default_factory=list)
    recent_signals: list[Signal] = field(default_factory=list)
    recent_trades: list[Trade] = field(default_factory=list)
    open_positions: list[Position] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def total_exposure_sol(self) -> float:
        return open_exposure_sol(self.open_positions)


def summarize_positions(positions: list[Position]) -> dict[str, float | int]:
    return {
        "open_count": len(positions),
        "open_exposure_sol": open_exposure_sol(positions),
    }


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    return Path(os.getenv("MEMECOIN_DB_PATH") or os.getenv("DATABASE_PATH") or DEFAULT_DB_PATH)


def load_dashboard_snapshot(
    settings: Settings,
    db_path: str | Path | None = None,
    *,
    signal_limit: int = 5,
    trade_limit: int = 5,
) -> DashboardSnapshot:
    snapshot = DashboardSnapshot(
        captured_at=datetime.now(UTC),
        execution_mode=settings.execution.mode,
        db_path=resolve_db_path(db_path),
    )
    snapshot.component_health, snapshot.stale_components, health_warnings = collect_health_snapshot(
        settings.monitoring.heartbeat_interval_s * 2
    )
    snapshot.warnings.extend(health_warnings)

    if not snapshot.db_path.exists():
        snapshot.warnings.append(f"Database not found at {snapshot.db_path}")
        return snapshot

    try:
        snapshot.recent_trades = load_recent_trades(snapshot.db_path, limit=trade_limit)
        snapshot.open_positions = load_open_positions(snapshot.db_path)
        snapshot.recent_signals = load_recent_signals(snapshot.db_path, limit=signal_limit)
    except sqlite3.Error as exc:
        snapshot.warnings.append(f"Database query failed: {exc}")

    if not snapshot.recent_signals:
        snapshot.warnings.append("No persisted signals available")

    return snapshot


def collect_health_snapshot(max_staleness_s: int) -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    warnings: list[str] = []
    monitor_cls = getattr(health_module, "HealthMonitor", None)
    if monitor_cls is not None:
        try:
            monitor = monitor_cls(max_staleness_s=max_staleness_s)
            return monitor.status(), list(monitor.stale_components()), warnings
        except Exception as exc:  # pragma: no cover - defensive against future implementations
            warnings.append(f"HealthMonitor failed to initialize: {exc}")

    status_fn = getattr(health_module, "check_health", None)
    if callable(status_fn):
        health_status = status_fn()
        return {
            "monitoring": {
                "ok": getattr(health_status, "ok", False),
                "message": getattr(health_status, "message", "unknown"),
                "checked_at": getattr(health_status, "checked_at", None),
            }
        }, [], [SUPPORTED_HEALTH_INTERFACE]

    return {}, [], ["No health interface available in src.monitoring.health"]


def load_recent_trades(db_path: Path, *, limit: int = 5) -> list[Trade]:
    rows = query_rows(
        db_path,
        (
            "SELECT metadata_json FROM trades ORDER BY executed_at DESC LIMIT ?",
            "SELECT * FROM trades ORDER BY executed_at DESC LIMIT ?",
        ),
        limit,
    )
    return [parse_trade(row) for row in rows]


def load_open_positions(db_path: Path) -> list[Position]:
    rows = query_rows(
        db_path,
        (
            "SELECT partial_exits_json FROM positions WHERE status != 'CLOSED' ORDER BY opened_at DESC",
            "SELECT * FROM positions WHERE status != 'CLOSED' ORDER BY opened_at DESC",
        ),
    )
    return [parse_position(row) for row in rows]


def load_recent_signals(db_path: Path, *, limit: int = 5) -> list[Signal]:
    rows = query_rows(
        db_path,
        (
            "SELECT metadata_json FROM signals ORDER BY observed_at DESC LIMIT ?",
            "SELECT payload_json FROM signals ORDER BY observed_at DESC LIMIT ?",
            "SELECT * FROM signals ORDER BY observed_at DESC LIMIT ?",
            "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?",
        ),
        limit,
        ignore_errors=True,
    )
    signals: list[Signal] = []
    for row in rows:
        try:
            signals.append(parse_signal(row))
        except Exception:
            continue
    return signals


def query_rows(
    db_path: Path,
    statements: tuple[str, ...],
    *params: Any,
    ignore_errors: bool = False,
) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        for statement in statements:
            try:
                rows = connection.execute(statement, params).fetchall()
                return rows
            except sqlite3.Error:
                if ignore_errors:
                    continue
                raise
    return []


def parse_trade(row: sqlite3.Row) -> Trade:
    payload = row_to_payload(row, ("metadata_json",))
    return Trade.model_validate(payload)


def parse_position(row: sqlite3.Row) -> Position:
    payload = row_to_payload(row, ("partial_exits_json",))
    return Position.model_validate(payload)


def parse_signal(row: sqlite3.Row) -> Signal:
    payload = row_to_payload(row, ("metadata_json", "payload_json"))
    payload.setdefault("source", SignalSource.MANUAL)
    payload.setdefault("type", SignalType.MENTION)
    payload.setdefault("confidence", 0.0)
    payload.setdefault("weight", 1.0)
    return Signal.model_validate(payload)


def row_to_payload(row: sqlite3.Row, json_fields: tuple[str, ...]) -> dict[str, Any]:
    for field_name in json_fields:
        if field_name in row.keys() and row[field_name]:
            return json.loads(row[field_name])
    return {key: row[key] for key in row.keys()}


def build_layout(snapshot: DashboardSnapshot) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(build_header(snapshot), name="header", size=5),
        Layout(name="body"),
        Layout(build_footer(snapshot), name="footer", size=6),
    )
    layout["body"].split_row(
        Layout(build_health_panel(snapshot), name="health", ratio=2),
        Layout(name="streams", ratio=3),
    )
    layout["streams"].split_column(
        Layout(build_positions_panel(snapshot), name="positions"),
        Layout(build_signals_trades_panel(snapshot), name="activity"),
    )
    return layout


def build_header(snapshot: DashboardSnapshot) -> Panel:
    warning_style = "bold white on red" if snapshot.execution_mode.lower() == "live" else "bold white on dark_green"
    mode_text = Text(f"EXECUTION MODE: {snapshot.execution_mode.upper()}", style=warning_style)
    summary = Text(
        f"Open positions: {len(snapshot.open_positions)}  |  Exposure: {snapshot.total_exposure_sol:.4f} SOL  |  DB: {snapshot.db_path}",
        style="bold",
    )
    return Panel(Group(mode_text, summary), title="Memecoin Trader Dashboard")


def build_health_panel(snapshot: DashboardSnapshot) -> Panel:
    table = Table(expand=True)
    table.add_column("Component")
    table.add_column("Status")
    table.add_column("Checked")
    table.add_column("Message")

    if not snapshot.component_health:
        table.add_row("monitoring", "unknown", "-", "No health data available")
    else:
        for component, details in snapshot.component_health.items():
            checked_at = format_timestamp(details.get("checked_at"))
            is_ok = bool(details.get("ok"))
            status = "healthy" if is_ok else "unhealthy"
            style = "green" if is_ok else "red"
            table.add_row(component, f"[{style}]{status}[/{style}]", checked_at, str(details.get("message", "")))

    stale = ", ".join(snapshot.stale_components) if snapshot.stale_components else "None"
    return Panel(Group(table, Text(f"Stale or missing heartbeats: {stale}", style="yellow")), title="Component Health")


def build_positions_panel(snapshot: DashboardSnapshot) -> Panel:
    table = Table(expand=True)
    table.add_column("Mint")
    table.add_column("Status")
    table.add_column("Amount SOL", justify="right")
    table.add_column("Entry", justify="right")
    table.add_column("PnL", justify="right")

    if not snapshot.open_positions:
        table.add_row("-", "-", "0", "0", "0")
    else:
        for position in snapshot.open_positions:
            table.add_row(
                shorten(position.mint_address),
                position.status.value,
                f"{position.amount_sol:.4f}",
                f"{position.entry_price_sol:.6f}",
                f"{position.realized_pnl_sol:.4f}",
            )
    return Panel(table, title="Open Positions")


def build_signals_trades_panel(snapshot: DashboardSnapshot) -> Panel:
    signals = Table(expand=True)
    signals.add_column("Signal")
    signals.add_column("Mint")
    signals.add_column("Confidence", justify="right")

    if not snapshot.recent_signals:
        signals.add_row("No signals", "-", "-")
    else:
        for item in snapshot.recent_signals:
            signals.add_row(item.type.value, shorten(item.mint_address), f"{item.confidence:.2f}")

    trades = Table(expand=True)
    trades.add_column("Side")
    trades.add_column("Mint")
    trades.add_column("Amount", justify="right")
    trades.add_column("Mode")

    if not snapshot.recent_trades:
        trades.add_row("No trades", "-", "-", snapshot.execution_mode)
    else:
        for trade in snapshot.recent_trades:
            trades.add_row(trade.side.value, shorten(trade.mint_address), f"{trade.amount_sol:.4f}", trade.mode)

    return Panel(Group(Text("Recent Signals", style="bold cyan"), signals, Text("Recent Trades", style="bold magenta"), trades), title="Recent Activity")


def build_footer(snapshot: DashboardSnapshot) -> Panel:
    warnings = snapshot.warnings or ["No active warnings"]
    text = Text()
    for warning in warnings:
        text.append(f"- {warning}\n", style="yellow")
    text.append(f"Last refresh: {format_timestamp(snapshot.captured_at)}", style="dim")
    return Panel(text, title="Warnings")


def format_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%H:%M:%SZ")
    if isinstance(value, str):
        return value.replace("T", " ")[:19]
    return "-"


def shorten(value: str, *, width: int = 12) -> str:
    if len(value) <= width:
        return value
    return f"{value[:6]}...{value[-4:]}"


def run_dashboard(*, interval_s: float | None = None, db_path: str | Path | None = None, once: bool = False) -> None:
    settings = load_settings()
    refresh_interval = interval_s or max(1.0, float(settings.monitoring.heartbeat_interval_s))
    stop_requested = False

    def stop_handler(_: int, __: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    try:
        snapshot = load_dashboard_snapshot(settings, db_path)
        with Live(build_layout(snapshot), refresh_per_second=4, screen=False) as live:
            if once:
                return
            while not stop_requested:
                time.sleep(refresh_interval)
                snapshot = load_dashboard_snapshot(settings, db_path)
                live.update(build_layout(snapshot))
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Memecoin Trader monitoring dashboard")
    parser.add_argument("--interval", type=float, default=None, help="Refresh interval in seconds")
    parser.add_argument("--db-path", default=None, help="Override SQLite database path")
    parser.add_argument("--once", action="store_true", help="Render one frame and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_dashboard(interval_s=args.interval, db_path=args.db_path, once=args.once)


if __name__ == "__main__":
    main()
