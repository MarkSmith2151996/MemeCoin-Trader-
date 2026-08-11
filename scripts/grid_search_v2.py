#!/usr/bin/env python3
"""Read-only MT-520 walk-forward parameter search for the paper strategies.

The search uses recorded price paths when position-linked snapshots exist and
falls back to the persisted entry/peak/close summary for older positions. Gate
tests intentionally use only candidates tied to entered positions: rejected
candidates have no subsequent price path, so their counterfactual PnL is not
invented.
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from statistics import fmean, pstdev


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trades.db"
OUTPUT_DIR = ROOT / "analysis"
JSON_PATH = OUTPUT_DIR / "grid_search_v2_results.json"
REPORT_PATH = OUTPUT_DIR / "grid_search_v2.md"

HOLDER_VALUES = (80, 85, 90, 95, 100)
TRAIL_VALUES = (3, 4, 5, 6)
TAKE_PROFIT_VALUES = (40, 50, 60, 70, 80)
HARD_STOP_VALUES = (8, 10, 12)
EARLY_TIMEOUT_VALUES = (60, 90, 120)
EARLY_THRESHOLD_VALUES = (1, 2, 3)
COOLDOWN_VALUES = (1, 2, 4)
CONCURRENT_VALUES = (3, 4, 5)

MIN_MCAP_VALUES = (1_000, 2_000, 5_000, 10_000)
MIN_VOLUME_VALUES = (100, 200, 500, 1_000)
MIN_TXNS_VALUES = (3, 5, 8, 12, 16)
MAX_AGE_VALUES = (10, 15, 20, 30)
WINDOW_COUNT = 4


@dataclass(frozen=True)
class PositionRecord:
    id: str
    strategy: str
    opened_at: str
    entry: float
    peak: float
    close: float
    amount: float
    snapshots: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class ExitParameters:
    trailing_stop_pct: int
    take_profit_pct: int
    hard_stop_pct: int
    early_exit_timeout_s: int
    early_exit_threshold_pct: int


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_positions() -> list[PositionRecord]:
    """Load valid closed positions and their optional persisted mark paths."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT id, strategy, opened_at, entry_price_sol,
                   COALESCE(NULLIF(peak_price_sol, 0), entry_price_sol),
                   COALESCE(close_price_sol, entry_price_sol * (1 + realized_pnl_sol / amount_sol)),
                   amount_sol
            FROM positions
            WHERE status = 'CLOSED'
              AND entry_price_sol > 0
              AND amount_sol > 0
            ORDER BY opened_at, id
            """,
        ).fetchall()
        snapshot_rows = conn.execute(
            """
            SELECT position_id, observed_at, price_sol
            FROM price_snapshots
            WHERE position_id IS NOT NULL AND price_sol > 0
            ORDER BY position_id, observed_at
            """,
        ).fetchall()
    finally:
        conn.close()

    snapshots: dict[str, list[tuple[str, float]]] = {}
    for position_id, observed_at, price in snapshot_rows:
        snapshots.setdefault(str(position_id), []).append((str(observed_at), float(price)))

    records: list[PositionRecord] = []
    for position_id, strategy, opened_at, entry, peak, close, amount in rows:
        opened = parse_timestamp(str(opened_at))
        points = [(0.0, float(entry))]
        for observed_at, price in snapshots.get(str(position_id), []):
            elapsed = (parse_timestamp(observed_at) - opened).total_seconds()
            if elapsed >= 0:
                points.append((elapsed, price))
        points.append((math.inf, float(close)))
        records.append(
            PositionRecord(
                id=str(position_id), strategy=str(strategy), opened_at=str(opened_at),
                entry=float(entry), peak=float(peak), close=float(close), amount=float(amount),
                snapshots=tuple(sorted(points)),
            ),
        )
    return records


def build_windows(records: list[PositionRecord]) -> list[tuple[int, int, int, int]]:
    """Create four overlapping chronological 50%-train / 25%-test folds."""
    train_size = max(1, len(records) // 2)
    test_size = max(1, len(records) // 4)
    max_start = max(0, len(records) - train_size - test_size)
    starts = [round(max_start * index / (WINDOW_COUNT - 1)) for index in range(WINDOW_COUNT)]
    return [(start, start + train_size, start + train_size, start + train_size + test_size) for start in starts]


def simulate_position(record: PositionRecord, params: ExitParameters) -> float:
    """Return simulated SOL PnL, preferring ordered snapshot marks when present."""
    peak = record.entry
    for elapsed, price in record.snapshots:
        peak = max(peak, price)
        change_pct = (price / record.entry - 1) * 100
        if change_pct >= params.take_profit_pct:
            return record.amount * (params.take_profit_pct / 100)
        if change_pct <= -params.hard_stop_pct:
            return -record.amount * (params.hard_stop_pct / 100)
        if (
            peak > record.entry * 1.02
            and (peak - price) / peak * 100 >= params.trailing_stop_pct
        ):
            return record.amount * (price / record.entry - 1)
        if (
            elapsed != math.inf
            and elapsed >= params.early_exit_timeout_s
            and peak <= record.entry * (1 + params.early_exit_threshold_pct / 100)
        ):
            return record.amount * (price / record.entry - 1)

    # Older records do not have a mark path. Keep the MT-502 peak-bound exit
    # model for these rows rather than fabricating intratrade timestamps.
    if record.peak >= record.entry * (1 + params.take_profit_pct / 100):
        exit_price = record.entry * (1 + params.take_profit_pct / 100)
    else:
        exit_price = min(record.close, record.peak * (1 - params.trailing_stop_pct / 100))
        exit_price = max(exit_price, record.entry * (1 - params.hard_stop_pct / 100))
    return record.amount * (exit_price / record.entry - 1)


def metrics(pnls: list[float]) -> dict[str, float | int]:
    if not pnls:
        return {"trades": 0, "pnl_sol": 0.0, "win_rate": 0.0, "sharpe": 0.0, "max_drawdown_sol": 0.0}
    equity = 0.0
    high = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        high = max(high, equity)
        max_drawdown = min(max_drawdown, equity - high)
    deviation = pstdev(pnls)
    sharpe = 0.0 if deviation == 0 else fmean(pnls) / deviation * math.sqrt(len(pnls))
    return {
        "trades": len(pnls), "pnl_sol": round(sum(pnls), 8),
        "win_rate": round(sum(pnl > 0 for pnl in pnls) / len(pnls), 6),
        "sharpe": round(sharpe, 6), "max_drawdown_sol": round(max_drawdown, 8),
    }


def exit_grid(records: list[PositionRecord], baseline: ExitParameters) -> dict[str, object]:
    """Evaluate each requested exit combination against all four test folds."""
    windows = build_windows(records)
    baseline_windows = [
        metrics([simulate_position(record, baseline) for record in records[test_start:test_end]])
        for _, _, test_start, test_end in windows
    ]
    results: list[dict[str, object]] = []
    for values in product(
        TRAIL_VALUES, TAKE_PROFIT_VALUES, HARD_STOP_VALUES,
        EARLY_TIMEOUT_VALUES, EARLY_THRESHOLD_VALUES,
    ):
        params = ExitParameters(*values)
        window_metrics = [
            metrics([simulate_position(record, params) for record in records[test_start:test_end]])
            for _, _, test_start, test_end in windows
        ]
        test_pnls = [float(item["pnl_sol"]) for item in window_metrics]
        results.append({"parameters": asdict(params), "windows": window_metrics, "mean_test_pnl_sol": round(fmean(test_pnls), 8)})

    for item in results:
        item["window_wins"] = sum(
            float(item["windows"][index]["pnl_sol"]) > float(baseline_windows[index]["pnl_sol"])
            for index in range(WINDOW_COUNT)
        )
        test_pnls = [
            simulate_position(record, params)
            for _, _, test_start, test_end in windows
            for record in records[test_start:test_end]
        ]
        item["aggregate_test_metrics"] = metrics(test_pnls)
    for item in results:
        params = item["parameters"]
        neighbors = [
            candidate for candidate in results
            if abs(candidate["parameters"]["trailing_stop_pct"] - params["trailing_stop_pct"]) <= 1
            and abs(candidate["parameters"]["take_profit_pct"] - params["take_profit_pct"]) <= 10
            and candidate["parameters"]["hard_stop_pct"] == params["hard_stop_pct"]
            and candidate["parameters"]["early_exit_timeout_s"] == params["early_exit_timeout_s"]
            and candidate["parameters"]["early_exit_threshold_pct"] == params["early_exit_threshold_pct"]
        ]
        item["sensitivity_positive_fraction"] = round(
            sum(float(neighbor["mean_test_pnl_sol"]) > 0 for neighbor in neighbors) / len(neighbors), 6,
        )
    results.sort(key=lambda item: (int(item["window_wins"]), float(item["mean_test_pnl_sol"])), reverse=True)
    winner = results[0]
    majority = int(winner["window_wins"]) >= WINDOW_COUNT // 2 + 1
    confidence = "HIGH" if majority and float(winner["sensitivity_positive_fraction"]) >= 0.8 else "MEDIUM" if majority else "LOW"
    return {
        "trade_count": len(records), "windows": [
            {"train_rows": [train_start + 1, train_end], "test_rows": [test_start + 1, test_end]}
            for train_start, train_end, test_start, test_end in windows
        ],
        "baseline_parameters": asdict(baseline), "baseline_windows": baseline_windows,
        "winner": winner, "top_5": results[:5], "majority_window_winner": majority,
        "confidence": confidence, "full_results": results,
    }


def load_strategy_b_candidate_outcomes(records: list[PositionRecord]) -> list[tuple[PositionRecord, dict[str, float]]]:
    by_id = {record.id: record for record in records if record.strategy == "B"}
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT position_id, age_minutes, mcap_usd, volume_usd, txns_buys, txns_sells
            FROM candidate_log
            WHERE strategy = 'B' AND entered = TRUE AND position_id IS NOT NULL
            """,
        ).fetchall()
    finally:
        conn.close()
    outcomes = []
    for position_id, age, mcap, volume, buys, sells in rows:
        record = by_id.get(str(position_id))
        values = (age, mcap, volume, buys, sells)
        if record is None or any(value is None for value in values):
            continue
        outcomes.append((record, {
            "max_age_minutes": float(age), "min_mcap_usd": float(mcap),
            "min_volume_usd": float(volume), "min_txns": float(buys) + float(sells),
        }))
    return outcomes


def gate_grid(records: list[PositionRecord], exit_params: ExitParameters) -> dict[str, object]:
    """Score gate-widths only where subsequent realized outcomes are known."""
    outcomes = load_strategy_b_candidate_outcomes(records)
    windows = build_windows(records)
    index_by_id = {record.id: index for index, record in enumerate(records)}
    baseline_windows: list[dict[str, float | int]] = []
    for _, _, test_start, test_end in windows:
        baseline_pnls = [
            simulate_position(record, exit_params)
            for record, values in outcomes
            if test_start <= index_by_id[record.id] < test_end
            and values["max_age_minutes"] <= 30 and values["min_mcap_usd"] >= 2_000
            and values["min_volume_usd"] >= 200 and values["min_txns"] >= 3
        ]
        baseline_windows.append(metrics(baseline_pnls))
    results: list[dict[str, object]] = []
    for max_age, min_mcap, min_volume, min_txns in product(
        MAX_AGE_VALUES, MIN_MCAP_VALUES, MIN_VOLUME_VALUES, MIN_TXNS_VALUES,
    ):
        selected = [
            (record, values) for record, values in outcomes
            if values["max_age_minutes"] <= max_age and values["min_mcap_usd"] >= min_mcap
            and values["min_volume_usd"] >= min_volume and values["min_txns"] >= min_txns
        ]
        window_metrics = []
        valid = True
        for _, _, test_start, test_end in windows:
            pnls = [
                simulate_position(record, exit_params)
                for record, _ in selected
                if test_start <= index_by_id[record.id] < test_end
            ]
            if len(pnls) < 10:
                valid = False
            window_metrics.append(metrics(pnls))
        if valid:
            pnls = [simulate_position(record, exit_params) for record, _ in selected]
            result = metrics(pnls)
            result.update({
                "parameters": {"max_age_minutes": max_age, "min_mcap_usd": min_mcap,
                               "min_volume_usd": min_volume, "min_txns": min_txns},
                "windows": window_metrics,
                "mean_test_pnl_sol": round(fmean(float(item["pnl_sol"]) for item in window_metrics), 8),
            })
            results.append(result)
    for item in results:
        item["window_wins"] = sum(
            float(item["windows"][index]["pnl_sol"]) / max(1, int(item["windows"][index]["trades"]))
            > float(baseline_windows[index]["pnl_sol"]) / max(1, int(baseline_windows[index]["trades"]))
            for index in range(WINDOW_COUNT)
        )
    results.sort(key=lambda item: (int(item["window_wins"]), float(item["mean_test_pnl_sol"])), reverse=True)
    return {
        "linked_entered_outcomes": len(outcomes), "valid_combinations": len(results),
        "baseline_parameters": {"max_age_minutes": 30, "min_mcap_usd": 2_000,
                                "min_volume_usd": 200, "min_txns": 3},
        "baseline_windows": baseline_windows, "top_5": results[:5], "winner": results[0] if results else None,
        "full_results": results,
        "limitation": "Rejected candidates have no observed later price path, so widening gates cannot be counterfactually scored.",
    }


def current_settings() -> dict[str, dict[str, object]]:
    return {
        "A": {"max_top10_holder_pct": 80, "trailing_stop_pct": 4, "take_profit_pct": 60,
              "hard_stop_pct": 10, "early_exit_timeout_s": 90, "early_exit_threshold_pct": 1,
              "repeat_loser_cooldown_hours": 2, "max_concurrent_positions": 4},
        "B": {"max_age_minutes": 30, "min_mcap_usd": 2_000, "min_volume_usd": 200,
              "min_txns": 3, "trailing_stop_pct": None, "take_profit_pct": 100,
              "hard_stop_pct": 30, "early_exit_timeout_s": 90, "early_exit_threshold_pct": 1},
    }


def report(results: dict[str, object]) -> str:
    lines = ["# MT-520 Grid Search V2", "", "## Method", "",
             "Four chronological overlapping walk-forward folds use a 50% training and 25% test slice.",
             "The ranking uses test-fold PnL; a selected winner must beat its current baseline on at least 3 of 4 folds.",
             "Price-linked snapshots are replayed in timestamp order when available; older positions use the MT-502 peak-bound fallback.",
             "", "## Current Settings", "", "```json", json.dumps(results["current_settings"], indent=2), "```"]
    for strategy in ("A", "B"):
        data = results["strategies"][strategy]
        exit_data = data["exit_search"]
        lines.extend(["", f"## Strategy {strategy}", "", f"Closed positions analyzed: {exit_data['trade_count']}",
                      f"Exit confidence: **{exit_data['confidence']}**. Majority-window winner: **{exit_data['majority_window_winner']}**.",
                      "", "### Top 5 Exit Combinations", "",
                      "| Rank | Trailing | TP | Hard stop | Early timeout | Early threshold | Mean test PnL | Window wins | Win rate | Sharpe | Max drawdown |",
                      "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"])
        for rank, item in enumerate(exit_data["top_5"], 1):
            parameters = item["parameters"]
            aggregate = item["aggregate_test_metrics"]
            lines.append(
                f"| {rank} | {parameters['trailing_stop_pct']}% | {parameters['take_profit_pct']}% | "
                f"{parameters['hard_stop_pct']}% | {parameters['early_exit_timeout_s']}s | "
                f"{parameters['early_exit_threshold_pct']}% | {item['mean_test_pnl_sol']:+.5f} | "
                f"{item['window_wins']}/4 | {aggregate['win_rate']:.1%} | {aggregate['sharpe']:.2f} | {aggregate['max_drawdown_sol']:+.5f} |",
            )
        winner = exit_data["winner"]
        lines.extend(["", "### Sensitivity", "",
                      f"The winner's +/-1% trailing and +/-10% TP neighborhood has **{winner['sensitivity_positive_fraction']:.1%}** positive combinations.",
                      ""])
        if strategy == "A":
            lines.extend(["### Entry Controls", "",
                          "Strategy A does not persist holder concentration or rejected candidate outcomes. "
                          "`MAX_TOP10_HOLDER_PCT`, cooldown, and concurrency therefore remain at their current conservative values (80%, 2h, 4). "
                          "Changing them from this dataset would be unsupported.", ""])
        else:
            gate_data = data["gate_search"]
            lines.extend(["### Gate Width Search", "", gate_data["limitation"],
                          f"Linked entered outcomes: {gate_data['linked_entered_outcomes']}; valid combinations: {gate_data['valid_combinations']}.", "",
                          "| Rank | Max age | Min mcap | Min volume | Min txns | Trades | Mean test PnL | Window wins | Win rate | Sharpe | Max drawdown |",
                          "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"])
            for rank, item in enumerate(gate_data["top_5"], 1):
                parameters = item["parameters"]
                lines.append(
                    f"| {rank} | {parameters['max_age_minutes']}m | ${parameters['min_mcap_usd']:,} | "
                    f"${parameters['min_volume_usd']:,} | {parameters['min_txns']} | {item['trades']} | "
                    f"{item['mean_test_pnl_sol']:+.5f} | {item['window_wins']}/4 | {item['win_rate']:.1%} | "
                    f"{item['sharpe']:.2f} | {item['max_drawdown_sol']:+.5f} |",
                )
    lines.extend(["", "## Recommendation", "", json.dumps(results["recommendation"], indent=2), "",
                  "Confidence is limited by peak-bound fallback for historical paths and by the absence of outcomes for rejected candidates. "
                  "The result is an evidence-based baseline, not proof that widened gates would have won."])
    return "\n".join(lines) + "\n"


def main() -> None:
    records = load_positions()
    by_strategy = {strategy: [record for record in records if record.strategy == strategy] for strategy in ("A", "B")}
    baselines = {
        "A": ExitParameters(4, 60, 10, 90, 1),
        # 100% trailing disables the stop, matching the pre-MT-520 Strategy B loop.
        "B": ExitParameters(100, 100, 30, 90, 1),
    }
    strategy_results = {
        strategy: {"exit_search": exit_grid(strategy_records, baselines[strategy])}
        for strategy, strategy_records in by_strategy.items()
    }
    b_winner = strategy_results["B"]["exit_search"]["winner"]
    strategy_results["B"]["gate_search"] = gate_grid(by_strategy["B"], ExitParameters(**b_winner["parameters"]))
    results = {
        "methodology": {"window_count": WINDOW_COUNT, "exit_grid_size": 540,
                        "price_path_fallback": "persisted entry/peak/close peak-bound approximation"},
        "current_settings": current_settings(), "strategies": strategy_results,
        "recommendation": {
            "A": {"exit_parameters": strategy_results["A"]["exit_search"]["winner"]["parameters"],
                  "entry_controls": "Keep holder=80%, cooldown=2h, concurrent=4: no Strategy A entry-outcome evidence."},
            "B": {"exit_parameters": b_winner["parameters"],
                  "gate_parameters": strategy_results["B"]["gate_search"]["winner"]["parameters"] if strategy_results["B"]["gate_search"]["winner"] else None},
        },
    }
    OUTPUT_DIR.mkdir(exist_ok=True)
    JSON_PATH.write_text(json.dumps(results, indent=2) + "\n")
    REPORT_PATH.write_text(report(results))
    print(f"Wrote {REPORT_PATH}")
    print(f"Wrote {JSON_PATH}")


if __name__ == "__main__":
    main()
