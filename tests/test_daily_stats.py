"""Coverage for daily performance analytics (MT-526)."""

from __future__ import annotations

import asyncio

import pytest
import sqlite3
from pathlib import Path

from src.analytics.daily_stats import (
    DayStats,
    backfill,
    build_series,
    current_drawdown_sol,
    latest_stats,
    load_series,
    streak,
)
from src.core.database import DAILY_STATS_SCHEMA, init_db


def _insert_close(
    db_path: Path,
    position_id: str,
    *,
    strategy: str = "A",
    pnl: float = 0.0,
    closed_at: str,
) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """INSERT INTO positions (id, mint_address, entry_trade_id, amount_sol,
               token_amount, entry_price_sol, status, opened_at, closed_at,
               realized_pnl_sol, partial_exits_json, strategy)
               VALUES (?, 'MINT', 'tr', 0.05, 1, 1e-6, 'CLOSED', ?, ?, ?, '[]', ?)""",
            (position_id, closed_at, closed_at, pnl, strategy),
        )


def _make_day(
    date_str: str,
    total: float,
    *,
    cumulative: float,
    max_drawdown: float = 0.0,
    sharpe: float | None = None,
) -> DayStats:
    return DayStats(
        date=date_str,
        strategy_a_trades=1,
        strategy_a_pnl_sol=total,
        strategy_a_win_rate=1.0 if total > 0 else 0.0,
        strategy_b_trades=0,
        strategy_b_pnl_sol=0.0,
        strategy_b_win_rate=0.0,
        total_pnl_sol=total,
        cumulative_pnl_sol=cumulative,
        max_drawdown_sol=max_drawdown,
        sharpe_ratio=sharpe,
    )


def _row_for(series: list[DayStats], date_str: str) -> DayStats:
    matches = [s for s in series if s.date == date_str]
    assert matches, f"no daily_stats row for {date_str} in {[s.date for s in series]}"
    return matches[0]


def test_metrics_per_strategy_and_cumulative(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", strategy="A", pnl=0.10, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p2", strategy="A", pnl=-0.05, closed_at="2026-08-10T13:00:00+00:00")
    _insert_close(db_path, "p3", strategy="B", pnl=0.02, closed_at="2026-08-10T14:00:00+00:00")

    series = build_series(db_path)
    row = _row_for(series, "2026-08-10")
    assert row.strategy_a_trades == 2
    assert row.strategy_a_pnl_sol == 0.05
    assert row.strategy_a_win_rate == 0.5
    assert row.strategy_b_trades == 1
    assert row.strategy_b_pnl_sol == 0.02
    assert row.strategy_b_win_rate == 1.0
    assert row.total_pnl_sol == 0.07
    assert row.cumulative_pnl_sol == 0.07
    assert row.max_drawdown_sol == 0.0


def test_cumulative_and_drawdown_across_days(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", strategy="A", pnl=0.10, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p2", strategy="A", pnl=-0.08, closed_at="2026-08-11T12:00:00+00:00")

    series = build_series(db_path)
    assert _row_for(series, "2026-08-10").cumulative_pnl_sol == 0.10
    row = _row_for(series, "2026-08-11")
    assert row.cumulative_pnl_sol == pytest.approx(0.02)
    assert row.max_drawdown_sol == pytest.approx(0.08)


def test_max_drawdown_is_running_max_across_partial_recovery(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", pnl=0.10, closed_at="2026-08-09T12:00:00+00:00")
    _insert_close(db_path, "p2", pnl=-0.08, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p3", pnl=0.06, closed_at="2026-08-11T12:00:00+00:00")

    series = build_series(db_path)
    recovered = _row_for(series, "2026-08-11")
    assert recovered.cumulative_pnl_sol == pytest.approx(0.08)
    assert recovered.max_drawdown_sol == pytest.approx(0.08)  # still 0.08, never resets
    assert current_drawdown_sol(series) == pytest.approx(0.02)


def test_zero_trade_days_are_included(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", pnl=0.01, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p2", pnl=0.02, closed_at="2026-08-12T12:00:00+00:00")

    series = build_series(db_path)
    gap = _row_for(series, "2026-08-11")
    assert gap.strategy_a_trades == 0
    assert gap.total_pnl_sol == 0.0
    assert gap.strategy_a_win_rate == 0.0
    assert gap.cumulative_pnl_sol == 0.01
    dates = [s.date for s in series]
    assert dates == sorted(dates)
    assert "2026-08-11" in dates


def test_sharpe_needs_two_days_and_variance(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", pnl=0.10, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p2", pnl=-0.04, closed_at="2026-08-11T12:00:00+00:00")

    series = build_series(db_path)
    assert _row_for(series, "2026-08-10").sharpe_ratio is None  # single day in window
    assert _row_for(series, "2026-08-11").sharpe_ratio is not None  # two days, variance


def test_sharpe_null_on_zero_variance(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", pnl=0.05, closed_at="2026-08-09T12:00:00+00:00")
    _insert_close(db_path, "p2", pnl=0.05, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p3", pnl=0.05, closed_at="2026-08-11T12:00:00+00:00")

    series = build_series(db_path)
    assert _row_for(series, "2026-08-11").sharpe_ratio is None  # stdev == 0


def test_et_day_boundary(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    # 2026-08-10T03:30:00Z = 23:30 ET on Aug 9 (EDT = UTC-4)
    _insert_close(db_path, "p1", pnl=0.01, closed_at="2026-08-10T03:30:00+00:00")
    # 2026-08-10T04:30:00Z = 00:30 ET on Aug 10
    _insert_close(db_path, "p2", pnl=0.02, closed_at="2026-08-10T04:30:00+00:00")

    series = build_series(db_path)
    assert _row_for(series, "2026-08-09").total_pnl_sol == 0.01
    assert _row_for(series, "2026-08-10").total_pnl_sol == 0.02


def test_non_closed_and_null_closed_at_ignored(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", pnl=0.01, closed_at="2026-08-10T12:00:00+00:00")
    with sqlite3.connect(db_path) as db:
        db.execute(
            """INSERT INTO positions (id, mint_address, entry_trade_id, amount_sol,
               token_amount, entry_price_sol, status, opened_at, closed_at,
               realized_pnl_sol, partial_exits_json, strategy)
               VALUES ('open', 'MINT2', 'tr', 0.05, 1, 1e-6, 'OPEN', ?, NULL, 0.5, '[]', 'B')""",
            ("2026-08-10T12:00:00+00:00",),
        )
        db.execute(
            """INSERT INTO positions (id, mint_address, entry_trade_id, amount_sol,
               token_amount, entry_price_sol, status, opened_at, closed_at,
               realized_pnl_sol, partial_exits_json, strategy)
               VALUES ('null-close', 'MINT3', 'tr', 0.05, 1, 1e-6, 'CLOSED', ?, NULL, 0.9, '[]', 'B')""",
            ("2026-08-10T12:00:00+00:00",),
        )

    series = build_series(db_path)
    row = _row_for(series, "2026-08-10")
    assert row.strategy_a_trades == 1
    assert row.strategy_b_trades == 0
    assert row.total_pnl_sol == 0.01


def test_backfill_idempotent_and_load(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    _insert_close(db_path, "p1", strategy="A", pnl=0.10, closed_at="2026-08-10T12:00:00+00:00")
    _insert_close(db_path, "p2", strategy="B", pnl=-0.03, closed_at="2026-08-11T12:00:00+00:00")

    first = backfill(db_path)
    second = backfill(db_path)
    assert first == second
    assert load_series(db_path) == first
    assert latest_stats(db_path) == first[-1]
    with sqlite3.connect(db_path) as db:
        table = db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'daily_stats'"
        ).fetchone()
    assert table is not None
    assert "date TEXT PRIMARY KEY" in DAILY_STATS_SCHEMA


def test_streak_consecutive_days() -> None:
    g1 = _make_day("2026-08-01", 0.01, cumulative=0.01)
    g2 = _make_day("2026-08-02", 0.02, cumulative=0.03)
    r1 = _make_day("2026-08-03", -0.01, cumulative=0.02)
    flat = _make_day("2026-08-04", 0.0, cumulative=0.02)

    assert streak([]) == {"direction": "flat", "days": 0}
    assert streak([g1, g2]) == {"direction": "green", "days": 2}
    assert streak([g1, g2, r1]) == {"direction": "red", "days": 1}
    assert streak([g1, g2, flat]) == {"direction": "flat", "days": 0}
    assert streak([g1, r1, r1]) == {"direction": "red", "days": 2}
    assert streak([flat]) == {"direction": "flat", "days": 0}


def test_current_drawdown_from_peak() -> None:
    series = [
        _make_day("2026-08-01", 0.10, cumulative=0.10),
        _make_day("2026-08-02", 0.05, cumulative=0.15),
        _make_day("2026-08-03", -0.12, cumulative=0.03, max_drawdown=0.12),
    ]
    assert current_drawdown_sol(series) == 0.12
    assert current_drawdown_sol([]) == 0.0
    recovered = series + [_make_day("2026-08-04", 0.20, cumulative=0.23)]
    assert current_drawdown_sol(recovered) == 0.0
