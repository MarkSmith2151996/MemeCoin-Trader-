"""Daily performance analytics: equity curve, drawdown, Sharpe (MT-526).

Computes per-day strategy metrics from closed paper positions, builds a
continuous daily equity series (zero-trade days included) with cumulative
PnL, running max drawdown and a rolling 7-day Sharpe, and persists one row
per day into the ``daily_stats`` table.

Day boundaries follow ET (America/New_York), matching the Telegram bot's
daily summary so the midnight wire and the analytics series always agree.

Semantics:
  - ``date``          : ET calendar date, ``YYYY-MM-DD``
  - trades/PnL        : closed positions whose ``closed_at`` falls in that ET day
  - win rate          : wins / closed trades (0.0 when no trades that day)
  - cumulative PnL    : running sum of daily total PnL across all days
  - max drawdown      : largest peak-to-trough decline of cumulative PnL so far
  - sharpe ratio      : mean/std of the trailing 7-day daily-PnL window,
                        annualized by sqrt(365) (24/7 crypto trading); NULL
                        when fewer than 2 days or zero variance

``backfill()`` is idempotent (INSERT OR REPLACE) and safe to rerun at any
cadence; the Telegram bot calls ``scripts/run_daily_stats.py --today``
right before its ET-midnight summary.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import mean, stdev
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import aiosqlite

from src.core.database import DAILY_STATS_SCHEMA

ET_TZ = ZoneInfo("America/New_York")
SHARPE_WINDOW_DAYS = 7
SHARPE_ANNUALIZATION = 365**0.5


@dataclass(frozen=True, slots=True)
class DayStats:
    """One persisted daily_stats row."""

    date: str
    strategy_a_trades: int
    strategy_a_pnl_sol: float
    strategy_a_win_rate: float
    strategy_b_trades: int
    strategy_b_pnl_sol: float
    strategy_b_win_rate: float
    total_pnl_sol: float
    cumulative_pnl_sol: float
    max_drawdown_sol: float
    sharpe_ratio: float | None


def today_et() -> date:
    """Current ET calendar date (the day boundary used for the series)."""

    return datetime.now(ET_TZ).date()


def _et_date(closed_at: str) -> date:
    parsed = datetime.fromisoformat(closed_at)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(ET_TZ).date()


async def _closed_rows(db_path: str | Path) -> list[tuple[str | None, float | None, str | None]]:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT strategy, realized_pnl_sol, closed_at FROM positions"
            " WHERE status = 'CLOSED' AND closed_at IS NOT NULL"
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return rows


def _day_metrics(
    rows: Iterable[tuple[str | None, float | None, str | None]],
) -> tuple[int, float, float, int, float, float]:
    a_trades = a_pnl = a_wins = 0
    b_trades = b_pnl = b_wins = 0
    for strategy, pnl, _closed_at in rows:
        value = float(pnl) if pnl is not None else 0.0
        if (strategy or "A").upper() == "B":
            b_trades += 1
            b_pnl += value
            b_wins += 1 if value > 0 else 0
        else:
            a_trades += 1
            a_pnl += value
            a_wins += 1 if value > 0 else 0
    a_win_rate = a_wins / a_trades if a_trades else 0.0
    b_win_rate = b_wins / b_trades if b_trades else 0.0
    return a_trades, a_pnl, a_win_rate, b_trades, b_pnl, b_win_rate


def _sharpe(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    spread = stdev(values)
    if spread == 0:
        return None
    return (mean(values) / spread) * SHARPE_ANNUALIZATION


def build_series(db_path: str | Path) -> list[DayStats]:
    """Compute the full daily series from closed positions, today inclusive.

    Pure computation — nothing is persisted. Days with no closes get a
    zero-trade row so the equity curve is continuous.
    """

    closed = asyncio.run(_closed_rows(db_path))
    by_day: dict[date, list[tuple[str | None, float | None, str | None]]] = {}
    for row in closed:
        if row[2] is None:
            continue
        by_day.setdefault(_et_date(row[2]), []).append(row)
    if not by_day:
        return []

    series: list[DayStats] = []
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    window: list[float] = []
    day = min(by_day)
    while day <= today_et():
        a_trades, a_pnl, a_win_rate, b_trades, b_pnl, b_win_rate = _day_metrics(by_day.get(day, ()))
        total = a_pnl + b_pnl
        cumulative += total
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
        window.append(total)
        if len(window) > SHARPE_WINDOW_DAYS:
            window.pop(0)
        series.append(
            DayStats(
                date=day.isoformat(),
                strategy_a_trades=a_trades,
                strategy_a_pnl_sol=a_pnl,
                strategy_a_win_rate=a_win_rate,
                strategy_b_trades=b_trades,
                strategy_b_pnl_sol=b_pnl,
                strategy_b_win_rate=b_win_rate,
                total_pnl_sol=total,
                cumulative_pnl_sol=cumulative,
                max_drawdown_sol=max_drawdown,
                sharpe_ratio=_sharpe(window),
            )
        )
        day += timedelta(days=1)
    return series


async def _upsert_series(db_path: str | Path, series: Sequence[DayStats]) -> None:
    # Retry transient shared-DB locks at 100ms intervals rather than sleeping
    # through increasingly long blind backoff windows.
    deadline = asyncio.get_running_loop().time() + 6.0
    while True:
        try:
            async with aiosqlite.connect(db_path, timeout=0.1) as db:
                await db.execute("PRAGMA busy_timeout=100")
                await db.execute(DAILY_STATS_SCHEMA)
                for stats in series:
                    await db.execute(
                        """
                        INSERT OR REPLACE INTO daily_stats (
                            date, strategy_a_trades, strategy_a_pnl_sol, strategy_a_win_rate,
                            strategy_b_trades, strategy_b_pnl_sol, strategy_b_win_rate,
                            total_pnl_sol, cumulative_pnl_sol, max_drawdown_sol, sharpe_ratio
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            stats.date,
                            stats.strategy_a_trades,
                            stats.strategy_a_pnl_sol,
                            stats.strategy_a_win_rate,
                            stats.strategy_b_trades,
                            stats.strategy_b_pnl_sol,
                            stats.strategy_b_win_rate,
                            stats.total_pnl_sol,
                            stats.cumulative_pnl_sol,
                            stats.max_drawdown_sol,
                            stats.sharpe_ratio,
                        ),
                    )
                await db.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.1)


def backfill(db_path: str | Path) -> list[DayStats]:
    """Recompute and persist the full daily series (idempotent)."""

    series = build_series(db_path)
    if series:
        asyncio.run(_upsert_series(db_path, series))
    return series


def load_series(db_path: str | Path) -> list[DayStats]:
    """Read persisted rows ordered by date (oldest first); [] when absent."""

    async def _load() -> list[DayStats]:
        try:
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute(
                    """SELECT date, strategy_a_trades, strategy_a_pnl_sol, strategy_a_win_rate,
                              strategy_b_trades, strategy_b_pnl_sol, strategy_b_win_rate,
                              total_pnl_sol, cumulative_pnl_sol, max_drawdown_sol, sharpe_ratio
                       FROM daily_stats ORDER BY date"""
                )
                rows = await cursor.fetchall()
        except sqlite3.OperationalError:
            return []
        return [
            DayStats(
                date=str(row[0]),
                strategy_a_trades=int(row[1] or 0),
                strategy_a_pnl_sol=float(row[2] or 0.0),
                strategy_a_win_rate=float(row[3] or 0.0),
                strategy_b_trades=int(row[4] or 0),
                strategy_b_pnl_sol=float(row[5] or 0.0),
                strategy_b_win_rate=float(row[6] or 0.0),
                total_pnl_sol=float(row[7] or 0.0),
                cumulative_pnl_sol=float(row[8] or 0.0),
                max_drawdown_sol=float(row[9] or 0.0),
                sharpe_ratio=float(row[10]) if row[10] is not None else None,
            )
            for row in rows
        ]

    return asyncio.run(_load())


def latest_stats(db_path: str | Path) -> DayStats | None:
    """Most recent daily_stats row (today after a backfill), or None."""

    series = load_series(db_path)
    return series[-1] if series else None


def current_drawdown_sol(series: Sequence[DayStats]) -> float:
    """Decline from the peak cumulative PnL to the latest value (>= 0)."""

    if not series:
        return 0.0
    peak = max(stats.cumulative_pnl_sol for stats in series)
    return peak - series[-1].cumulative_pnl_sol


def streak(series: Sequence[DayStats]) -> dict[str, int | str]:
    """Consecutive green/red days ending at the latest row (flat breaks streaks)."""

    if not series:
        return {"direction": "flat", "days": 0}
    latest = series[-1].total_pnl_sol
    if latest == 0:
        return {"direction": "flat", "days": 0}
    direction = "green" if latest > 0 else "red"
    days = 0
    for stats in reversed(series):
        value = stats.total_pnl_sol
        if value == 0:
            break
        if (value > 0) == (latest > 0):
            days += 1
        else:
            break
    return {"direction": direction, "days": days}


def summarize(db_path: str | Path) -> dict[str, object]:
    """One-shot summary over persisted rows (used by scripts/run_daily_stats.py)."""

    series = load_series(db_path)
    if not series:
        return {
            "rows": [],
            "cumulative_pnl_sol": 0.0,
            "current_drawdown_sol": 0.0,
            "max_drawdown_sol": 0.0,
            "sharpe_ratio": None,
            "streak": {"direction": "flat", "days": 0},
        }
    last = series[-1]
    return {
        "rows": series,
        "cumulative_pnl_sol": last.cumulative_pnl_sol,
        "current_drawdown_sol": current_drawdown_sol(series),
        "max_drawdown_sol": last.max_drawdown_sol,
        "sharpe_ratio": last.sharpe_ratio,
        "streak": streak(series),
    }
