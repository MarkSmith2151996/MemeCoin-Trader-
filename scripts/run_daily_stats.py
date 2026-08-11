"""Backfill and print daily performance analytics (MT-526).

Usage:
  python3 scripts/run_daily_stats.py            # backfill all historical days, print full series
  python3 scripts/run_daily_stats.py --today    # backfill, print only today's row + summary

The Telegram bot also triggers ``--today`` right before its ET-midnight
daily summary, so the equity curve is always fresh when the summary sends.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.daily_stats import DayStats, backfill, summarize
from src.monitoring.dashboard import resolve_db_path


def _fmt_sol(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):.4f}"


def _fmt_sol_short(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):.2f}"


def _print_series(rows: list[DayStats]) -> None:
    header = (
        f"{'DATE':<12}{'A TR':>5}{'A PNL':>9}{'B TR':>5}{'B PNL':>9}"
        f"{'TOTAL':>9}{'CUM':>9}{'MAX DD':>9}{'SHARPE':>8}"
    )
    print(header)
    for row in rows:
        sharpe = f"{row.sharpe_ratio:.2f}" if row.sharpe_ratio is not None else "n/a"
        print(
            f"{row.date:<12}{row.strategy_a_trades:>5}{_fmt_sol(row.strategy_a_pnl_sol):>9}"
            f"{row.strategy_b_trades:>5}{_fmt_sol(row.strategy_b_pnl_sol):>9}"
            f"{_fmt_sol(row.total_pnl_sol):>9}{_fmt_sol(row.cumulative_pnl_sol):>9}"
            f"{_fmt_sol(row.max_drawdown_sol):>9}{sharpe:>8}"
        )


def _print_summary(summary: dict[str, object]) -> None:
    streak_state = summary["streak"]
    assert isinstance(streak_state, dict)
    streak_days = int(streak_state["days"])
    streak_text = f"{streak_days} {streak_state['direction']} day(s)" if streak_days else "flat"
    sharpe = summary["sharpe_ratio"]
    sharpe_text = f"{float(sharpe):.2f}" if isinstance(sharpe, float) else "n/a"
    print("\nSummary")
    print(f"  Cumulative PnL:           {_fmt_sol_short(float(summary['cumulative_pnl_sol']))} SOL")
    print(f"  Max drawdown (all-time):  -{abs(float(summary['max_drawdown_sol'])):.4f} SOL")
    print(f"  Current drawdown from peak: -{abs(float(summary['current_drawdown_sol'])):.4f} SOL")
    print(f"  7-day Sharpe:             {sharpe_text}")
    print(f"  Streak:                   {streak_text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill and print daily performance analytics (MT-526).")
    parser.add_argument("--today", action="store_true", help="backfill, then print only today's row + summary")
    parser.add_argument("--db", default=None, help="override SQLite DB path (default: env or data/trades.db)")
    args = parser.parse_args()

    db_path = resolve_db_path(args.db)
    series = backfill(db_path)
    print("=== Daily Stats (ET) ===")
    if not series:
        print("No closed positions yet — nothing to backfill.")
        return
    print(f"Backfilled {len(series)} day(s) through {series[-1].date} (db={db_path})\n")
    if args.today:
        _print_series([series[-1]])
        print("(full history in daily_stats — rerun without --today for all days)")
    else:
        _print_series(series)
    _print_summary(summarize(db_path))


if __name__ == "__main__":
    main()
