#!/usr/bin/env python3
"""Replay closed snapshot-backed positions without modifying the trading database."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Allow the documented `python3 scripts/run_backtest.py` invocation from any cwd.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.strategy.backtester import (  # noqa: E402, I001 - root added above for direct script use
    BacktestParameters,
    grid_search,
    load_closed_positions,
    summarize_backtest,
)


DEFAULT_DB_PATH = ROOT / "data" / "trades.db"
DEFAULT_OUTPUT_PATH = ROOT / "analysis" / "backtest_results.json"
DEFAULT_GRID = {
    "trailing_stop_pct": (2.0, 3.0, 4.0, 5.0, 6.0),
    "take_profit_pct": (40.0, 50.0, 60.0, 70.0, 80.0),
    "hard_stop_pct": (6.0, 8.0, 10.0, 12.0),
    "early_exit_timeout_s": (60.0, 90.0, 120.0),
    "early_exit_green_pct": (1.0, 2.0, 3.0),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path")
    parser.add_argument("--trail", type=float, default=3.0, help="Trailing stop percent")
    parser.add_argument("--tp", type=float, default=60.0, help="Take-profit percent")
    parser.add_argument("--hard", type=float, default=8.0, help="Hard-stop percent")
    parser.add_argument(
        "--trail-arm", type=float, default=2.0, help="Trailing arm gain percent",
    )
    parser.add_argument(
        "--early-timeout", type=float, default=90.0, help="Early-exit timeout seconds",
    )
    parser.add_argument(
        "--early-green", type=float, default=2.0, help="Required gain before early exit",
    )
    parser.add_argument(
        "--grid", action="store_true", help="Search the built-in exit parameter grid",
    )
    parser.add_argument(
        "--output",
        nargs="?",
        const=DEFAULT_OUTPUT_PATH,
        type=Path,
        help="Optionally write JSON results (default: analysis/backtest_results.json)",
    )
    return parser.parse_args()


def print_result(result: dict[str, object]) -> None:
    parameters = result["parameters"]
    simulated = result["simulated"]
    actual = result["actual"]
    print(f"Snapshot-backed closed positions: {result['eligible_positions']}")
    print(
        "Parameters: "
        f"trail={parameters['trailing_stop_pct']}% tp={parameters['take_profit_pct']}% "
        f"hard={parameters['hard_stop_pct']}% arm={parameters['trailing_arm_pct']}% "
        f"early={parameters['early_exit_timeout_s']}s/{parameters['early_exit_green_pct']}%",
    )
    print(
        "Simulated: "
        f"PnL {simulated['pnl_sol']:+.8f} SOL | win rate {simulated['win_rate']:.1%} | "
        f"Sharpe {simulated['sharpe']:.2f} | max drawdown {simulated['max_drawdown_sol']:+.8f} SOL",
    )
    print(
        "Actual:    "
        f"PnL {actual['pnl_sol']:+.8f} SOL | win rate {actual['win_rate']:.1%} | "
        f"Sharpe {actual['sharpe']:.2f} | max drawdown {actual['max_drawdown_sol']:+.8f} SOL",
    )
    print(f"Simulated vs actual PnL: {result['pnl_vs_actual_sol']:+.8f} SOL")
    print(f"Exit reasons: {json.dumps(result['exit_reasons'], sort_keys=True)}")


def main() -> None:
    args = parse_args()
    positions = load_closed_positions(args.db)
    parameters = BacktestParameters(
        trailing_stop_pct=args.trail,
        take_profit_pct=args.tp,
        hard_stop_pct=args.hard,
        trailing_arm_pct=args.trail_arm,
        early_exit_timeout_s=args.early_timeout,
        early_exit_green_pct=args.early_green,
    )
    if args.grid:
        results = grid_search(positions, DEFAULT_GRID, base_parameters=parameters)
        output: dict[str, object] = {
            "eligible_positions": len(positions),
            "grid_size": len(results),
            "results": results,
        }
        print(f"Snapshot-backed closed positions: {len(positions)}")
        print(f"Grid combinations: {len(results)}")
        for rank, result in enumerate(results[:10], start=1):
            print(f"\n#{rank}")
            print_result(result)
    else:
        output = summarize_backtest(positions, parameters)
        print_result(output)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(output, indent=2) + "\n")
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
