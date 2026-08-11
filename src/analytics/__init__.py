"""Daily performance analytics package (MT-526)."""

from src.analytics.daily_stats import (
    DayStats,
    backfill,
    build_series,
    current_drawdown_sol,
    latest_stats,
    load_series,
    streak,
    summarize,
    today_et,
)

__all__ = [
    "DayStats",
    "backfill",
    "build_series",
    "current_drawdown_sol",
    "latest_stats",
    "load_series",
    "streak",
    "summarize",
    "today_et",
]
