#!/usr/bin/env python3
"""Read-only Hive terminal dashboard for the V2 collector and executor."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.store import database_dsn  # noqa: E402

HEARTBEAT_PATH = Path("/tmp/memecoin-executor.heartbeat")

SUMMARY_SQL = """
SELECT
    COUNT(*) FILTER (WHERE status = 'open')::INTEGER AS open_positions,
    COALESCE(SUM(amount_sol) FILTER (WHERE status = 'open'), 0)::DOUBLE PRECISION
        AS open_exposure_sol,
    COUNT(*) FILTER (
        WHERE status = 'closed'
          AND closed_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    )::INTEGER AS today_closed_count,
    COALESCE(SUM(realized_pnl_sol) FILTER (
        WHERE status = 'closed'
          AND closed_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    ), 0)::DOUBLE PRECISION AS today_pnl_sol,
    COALESCE(SUM(realized_pnl_sol) FILTER (WHERE status = 'closed'), 0)::DOUBLE PRECISION
        AS all_time_pnl_sol
FROM memecoin.positions
"""
GATE_SQL = """
SELECT gate_name, gate_value, updated_at
FROM memecoin.gate_config
WHERE strategy = $1 AND enabled = TRUE
ORDER BY gate_name
"""
RECENT_TRADES_SQL = """
SELECT t.side, t.mint_address, t.amount_sol, t.price_sol, t.mode, t.executed_at,
       p.close_reason, p.realized_pnl_sol
FROM memecoin.trades AS t
LEFT JOIN memecoin.positions AS p ON p.id = t.position_id
ORDER BY t.executed_at DESC
LIMIT $1
"""
OPEN_POSITIONS_SQL = """
SELECT mint_address, mode, strategy, amount_sol, entry_price_sol, opened_at, status
FROM memecoin.positions
WHERE status = 'open'
ORDER BY opened_at DESC
"""
ACTIVITY_SQL = """
SELECT
    max(observed_at) FILTER (WHERE source LIKE 'jupiter_%') AS last_jupiter_candidate_at,
    max(observed_at) FILTER (WHERE source = 'pumpportal') AS last_pumpportal_candidate_at,
    COUNT(*) FILTER (WHERE observed_at >= NOW() - INTERVAL '5 minutes')::INTEGER
        AS candidate_observations_5m,
    COUNT(DISTINCT mint_address) FILTER (
        WHERE observed_at >= NOW() - INTERVAL '5 minutes'
    )::INTEGER AS candidate_mints_5m
FROM memecoin.candidates
"""
RUNTIME_EVENT_SQL = """
SELECT event_type, reason, occurred_at
FROM memecoin.runtime_events
ORDER BY occurred_at DESC
LIMIT 1
"""


@dataclass(frozen=True)
class HiveDashboardSnapshot:
    captured_at: datetime
    summary: dict[str, Any]
    gates: list[dict[str, Any]] = field(default_factory=list)
    trades: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    activity: dict[str, Any] = field(default_factory=dict)
    runtime_event: dict[str, Any] | None = None
    heartbeat: str = "missing"


def _shorten(value: object, width: int = 14) -> str:
    rendered = str(value or "-")
    return rendered if len(rendered) <= width else f"{rendered[:8]}...{rendered[-4:]}"


def _timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    return "-"


def _number(value: object, digits: int = 5) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "-"


def executor_heartbeat(path: Path = HEARTBEAT_PATH, *, now: datetime | None = None) -> str:
    """Return the local executor heartbeat state without changing service state."""

    try:
        payload = json.loads(path.read_text())
        last_cycle = datetime.fromisoformat(str(payload["last_cycle"]).replace("Z", "+00:00"))
        if last_cycle.tzinfo is None:
            return "invalid"
        age = max(0.0, ((now or datetime.now(UTC)) - last_cycle.astimezone(UTC)).total_seconds())
        return f"healthy ({age:.1f}s ago)" if age <= 30 else f"stale ({age:.1f}s ago)"
    except (KeyError, OSError, ValueError, TypeError, json.JSONDecodeError):
        return "missing"


async def load_snapshot(dsn: str, *, strategy: str = "BT") -> HiveDashboardSnapshot:
    """Read one snapshot through a PostgreSQL read-only transaction."""

    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction(readonly=True):
            summary = dict(await connection.fetchrow(SUMMARY_SQL))
            gates = [dict(row) for row in await connection.fetch(GATE_SQL, strategy)]
            trades = [dict(row) for row in await connection.fetch(RECENT_TRADES_SQL, 10)]
            positions = [dict(row) for row in await connection.fetch(OPEN_POSITIONS_SQL)]
            activity = dict(await connection.fetchrow(ACTIVITY_SQL))
            event = await connection.fetchrow(RUNTIME_EVENT_SQL)
    finally:
        await connection.close()
    return HiveDashboardSnapshot(
        captured_at=datetime.now(UTC),
        summary=summary,
        gates=gates,
        trades=trades,
        positions=positions,
        activity=activity,
        runtime_event=dict(event) if event else None,
        heartbeat=executor_heartbeat(),
    )


def _gate_value(value: object) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(value)


def build_dashboard(snapshot: HiveDashboardSnapshot) -> Group:
    summary = snapshot.summary
    header = Text(
        "V2 HIVE DASHBOARD (READ-ONLY)\n"
        f"executor heartbeat: {snapshot.heartbeat} | captured: {_timestamp(snapshot.captured_at)}\n"
        f"open: {summary.get('open_positions', 0)} | "
        f"exposure: {_number(summary.get('open_exposure_sol'))} SOL | "
        f"today: {_number(summary.get('today_pnl_sol'))} SOL "
        f"({summary.get('today_closed_count', 0)} closed) | "
        f"all-time: {_number(summary.get('all_time_pnl_sol'))} SOL",
        style="bold cyan",
    )

    status = Table(expand=True)
    status.add_column("Signal")
    status.add_column("Value")
    status.add_row(
        "Jupiter candidate",
        _timestamp(snapshot.activity.get("last_jupiter_candidate_at")),
    )
    status.add_row(
        "PumpPortal candidate",
        _timestamp(snapshot.activity.get("last_pumpportal_candidate_at")),
    )
    status.add_row(
        "Candidate activity (5m)",
        f"{snapshot.activity.get('candidate_observations_5m', 0)} observations / "
        f"{snapshot.activity.get('candidate_mints_5m', 0)} mints",
    )
    if snapshot.runtime_event:
        status.add_row(
            "Latest runtime event",
            f"{snapshot.runtime_event['event_type']} at "
            f"{_timestamp(snapshot.runtime_event['occurred_at'])}",
        )
        status.add_row("Event reason", _shorten(snapshot.runtime_event.get("reason"), 96))
    else:
        status.add_row("Latest runtime event", "none recorded")

    positions = Table(expand=True)
    positions.add_column("Mint")
    positions.add_column("Mode")
    positions.add_column("Strategy")
    positions.add_column("SOL", justify="right")
    positions.add_column("Entry", justify="right")
    positions.add_column("Opened")
    if snapshot.positions:
        for position in snapshot.positions:
            positions.add_row(
                _shorten(position.get("mint_address")),
                str(position.get("mode") or "-"),
                str(position.get("strategy") or "-"),
                _number(position.get("amount_sol")),
                _number(position.get("entry_price_sol"), 10),
                _timestamp(position.get("opened_at")),
            )
    else:
        positions.add_row("-", "-", "-", "-", "-", "No open positions")

    gates = Table(expand=True)
    gates.add_column("Enabled Gate")
    gates.add_column("Value")
    gates.add_column("Updated")
    if snapshot.gates:
        for gate in snapshot.gates:
            gates.add_row(
                str(gate.get("gate_name") or "-"),
                _gate_value(gate.get("gate_value")),
                _timestamp(gate.get("updated_at")),
            )
    else:
        gates.add_row("-", "No enabled BT gates", "-")

    trades = Table(expand=True)
    trades.add_column("At")
    trades.add_column("Side")
    trades.add_column("Mint")
    trades.add_column("SOL", justify="right")
    trades.add_column("Exit/PnL")
    if snapshot.trades:
        for trade in snapshot.trades:
            exit_detail = str(trade.get("close_reason") or "-")
            if trade.get("realized_pnl_sol") is not None:
                exit_detail += f" / {_number(trade['realized_pnl_sol'])}"
            trades.add_row(
                _timestamp(trade.get("executed_at")),
                str(trade.get("side") or "-"),
                _shorten(trade.get("mint_address")),
                _number(trade.get("amount_sol")),
                exit_detail,
            )
    else:
        trades.add_row("-", "-", "-", "-", "No entries or exits")

    return Group(
        Panel(header, title="Memecoin Trader"),
        Panel(status, title="Live Loop Status"),
        Panel(positions, title="Open Positions"),
        Panel(gates, title="Gate Stats"),
        Panel(trades, title="Recent Entries / Exits"),
    )


async def run_dashboard(*, interval: float, once: bool) -> None:
    dsn = database_dsn()
    snapshot = await load_snapshot(dsn)
    with Live(build_dashboard(snapshot), refresh_per_second=4, screen=False) as live:
        if once:
            return
        while True:
            await asyncio.sleep(interval)
            snapshot = await load_snapshot(dsn)
            live.update(build_dashboard(snapshot))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=5.0, help="Refresh interval in seconds")
    parser.add_argument("--once", action="store_true", help="Render one snapshot and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.interval <= 0:
        raise ValueError("--interval must be positive")
    load_dotenv(ROOT / ".env")
    asyncio.run(run_dashboard(interval=args.interval, once=args.once))


if __name__ == "__main__":
    main()
