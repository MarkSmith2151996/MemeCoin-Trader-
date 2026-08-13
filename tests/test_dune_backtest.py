"""Offline coverage for the detached Dune historical backtest script."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.dune_backtest import run


def write_csv(path: Path, columns: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def test_dune_backtest_filters_gates_and_replays_exit_order(tmp_path: Path) -> None:
    graduations = tmp_path / "graduated_tokens.csv"
    swaps = tmp_path / "graduated_token_swaps.csv"
    output = tmp_path / "output"
    write_csv(graduations, [
        "mint_address", "graduation_timestamp", "age_minutes_at_graduation",
        "market_cap_usd_at_graduation", "volume_usd_first_30m",
        "buy_count_first_30m", "sell_count_first_30m", "liquidity_added_usd_proxy",
    ], [
        {
            "mint_address": "passes", "graduation_timestamp": "2026-08-01T00:00:00Z",
            "age_minutes_at_graduation": 5, "market_cap_usd_at_graduation": 5_000,
            "volume_usd_first_30m": 1_000, "buy_count_first_30m": 8,
            "sell_count_first_30m": 4, "liquidity_added_usd_proxy": 100,
        },
        {
            "mint_address": "fails", "graduation_timestamp": "2026-08-01T00:00:00Z",
            "age_minutes_at_graduation": 5, "market_cap_usd_at_graduation": 500,
            "volume_usd_first_30m": 1_000, "buy_count_first_30m": 8,
            "sell_count_first_30m": 4, "liquidity_added_usd_proxy": 100,
        },
    ])
    write_csv(swaps, ["mint_address", "timestamp", "price_sol"], [
        {"mint_address": "passes", "timestamp": "2026-08-01T00:00:00Z", "price_sol": 1},
        {"mint_address": "passes", "timestamp": "2026-08-01T00:00:30Z", "price_sol": 1.03},
        {"mint_address": "passes", "timestamp": "2026-08-01T00:00:45Z", "price_sol": 0.98},
    ])

    summary = run(graduations, swaps, output, tmp_path / "missing.db")

    assert summary["dune_backtest"]["gate_passed"] == 1
    assert summary["dune_backtest"]["closed_within_two_hours"] == 1
    assert summary["dune_backtest"]["exit_reasons"] == {"trailing_stop": 1}
    assert summary["paper_trading_comparison"]["available"] is False

    with (output / "per_trade_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = {row["mint_address"]: row for row in csv.DictReader(handle)}
    assert rows["passes"]["exit_reason"] == "trailing_stop"
    assert rows["fails"]["gate_passed"] == "False"
    saved = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert saved["dune_backtest"]["total_realized_pnl_sol_at_0_05_size"] < 0
