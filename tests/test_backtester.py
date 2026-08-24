"""Offline coverage for snapshot-backed exit replay."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.core.database import init_db, record_position_price_snapshot
from src.strategy.backtester import (
    BacktestParameters,
    ClosedPosition,
    SnapshotPoint,
    grid_search,
    load_closed_positions,
    simulate_exit,
    summarize_backtest,
)


def test_simulate_exit_uses_runtime_exit_order() -> None:
    position = ClosedPosition(
        id="position-1",
        strategy="A",
        entry_price_sol=100.0,
        amount_sol=0.1,
        actual_pnl_sol=0.0,
        snapshots=(
            SnapshotPoint(elapsed_seconds=10, price_sol=110.0),
            SnapshotPoint(elapsed_seconds=20, price_sol=104.0),
        ),
    )

    result = simulate_exit(position, BacktestParameters(trailing_stop_pct=5.0))

    assert result.exit_reason == "trailing_stop"
    assert result.exit_price_sol == 104.0
    assert result.pnl_sol == 0.0040000000000000036


def test_simulate_exit_closes_no_green_positions_after_early_timeout() -> None:
    position = ClosedPosition(
        id="position-1",
        strategy="A",
        entry_price_sol=100.0,
        amount_sol=0.1,
        actual_pnl_sol=0.0,
        snapshots=(SnapshotPoint(elapsed_seconds=90, price_sol=101.0),),
    )

    result = simulate_exit(position, BacktestParameters(early_exit_green_pct=2.0))

    assert result.exit_reason == "early_exit_no_green"
    assert result.exit_price_sol == 101.0


def test_backtester_loads_only_closed_positions_with_linked_snapshots(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    opened = datetime(2026, 1, 1, tzinfo=UTC)
    with sqlite3.connect(db_path) as db:
        db.execute(
            """INSERT INTO positions (
                id, mint_address, entry_trade_id, amount_sol, token_amount,
                entry_price_sol, status, opened_at, closed_at, realized_pnl_sol,
                partial_exits_json, close_price_sol, peak_price_sol, strategy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "with-path", "Mint1", "trade-1", 0.1, 1.0, 100.0, "CLOSED", opened.isoformat(),
                (opened + timedelta(minutes=1)).isoformat(), 0.02, "[]", 120.0, 120.0, "A",
            ),
        )
        db.execute(
            """INSERT INTO positions (
                id, mint_address, entry_trade_id, amount_sol, token_amount,
                entry_price_sol, status, opened_at, closed_at, realized_pnl_sol,
                partial_exits_json, close_price_sol, peak_price_sol, strategy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "without-path", "Mint2", "trade-2", 0.1, 1.0, 100.0, "CLOSED", opened.isoformat(),
                (opened + timedelta(minutes=1)).isoformat(), -0.01, "[]", 90.0, 100.0, "A",
            ),
        )
        db.commit()
    asyncio.run(record_position_price_snapshot(
        db_path,
        position_id="with-path",
        mint_address="Mint1",
        price_sol=92.0,
        observed_at=opened + timedelta(seconds=10),
    ))

    positions = load_closed_positions(db_path)
    summary = summarize_backtest(positions, BacktestParameters(hard_stop_pct=8.0))

    assert [position.id for position in positions] == ["with-path"]
    assert summary["eligible_positions"] == 1
    assert summary["simulated"]["pnl_sol"] == -0.008
    assert summary["actual"]["pnl_sol"] == 0.02
    assert summary["exit_reasons"] == {"hard_stop": 1}


def test_grid_search_evaluates_requested_parameter_combinations() -> None:
    position = ClosedPosition(
        id="position-1",
        strategy="A",
        entry_price_sol=100.0,
        amount_sol=0.1,
        actual_pnl_sol=0.0,
        snapshots=(SnapshotPoint(elapsed_seconds=10, price_sol=105.0),),
    )

    results = grid_search([position], {"take_profit_pct": (4.0, 10.0)})

    assert len(results) == 2
    assert {result["parameters"]["take_profit_pct"] for result in results} == {4.0, 10.0}
    assert results[0]["parameters"]["take_profit_pct"] == 10.0
    assert results[0]["simulated"]["pnl_sol"] == 0.005
