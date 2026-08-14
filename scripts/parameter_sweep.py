#!/usr/bin/env python3
"""Read-only MT-552 parameter sweep backtest on closed Strategy B trades.

Replays every closed Strategy B position under a grid of trailing-stop /
take-profit / hard-stop combinations, then applies day/hour filters and a
mcap tier filter. Price-linked snapshots are replayed in timestamp order
where available (MT-520 pattern); older positions use the persisted
entry/peak/close peak-bound approximation. Pure analysis — never writes to
the database.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from statistics import fmean

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trades.db"
JSON_PATH = ROOT / "data" / "parameter_sweep_results.json"
REPORT_PATH = ROOT / "data" / "parameter_sweep_report.md"

# Fixed runtime exit behaviors from scripts/run_strategy_b.py (not swept).
TRAILING_ARM_PCT = 2.0
ENTRY_CONFIRM_WINDOW_S = 90
EARLY_EXIT_GREEN_PCT = 0.01
TIME_STOP_MINUTES = 10

# Sweep ranges (task MT-552 Step 2).
TRAIL_VALUES = (2, 3, 4, 5, 6, 8)
TAKE_PROFIT_VALUES = (60, 80, 100, 120, 150)
HARD_STOP_VALUES = (8, 10, 15, 20)

# Current live parameters (baseline).
BASELINE = {"trailing_stop_pct": 4, "take_profit_pct": 80, "hard_stop_pct": 10}

MCAP_FLOOR_USD = 20_000.0

GOLDEN_HOURS = frozenset({4, 5, 6, 8, 9, 10, 11, 12, 17})

FILTERS = {
    "baseline": "No filter",
    "no_wed": "Exclude Wednesday",
    "no_utc14": "Exclude UTC 14",
    "no_wed_no_utc14": "Exclude Wednesday + UTC 14",
    "thu_fri": "Only Thursday + Friday",
    "golden_hours": "Only golden hours (UTC 4-6, 8-12, 17)",
}


@dataclass(frozen=True, slots=True)
class PositionRecord:
    id: str
    opened_at: datetime
    entry: float
    peak: float
    close: float
    amount: float
    mcap_usd: float | None
    snapshots: tuple[tuple[float, float], ...]


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_positions() -> list[PositionRecord]:
    """Load closed Strategy B positions with optional snapshot paths and mcap."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT id, opened_at, entry_price_sol,
                   COALESCE(NULLIF(peak_price_sol, 0), entry_price_sol),
                   COALESCE(close_price_sol, entry_price_sol * (1 + realized_pnl_sol / amount_sol)),
                   amount_sol
            FROM positions
            WHERE status = 'CLOSED'
              AND strategy = 'B'
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
            ORDER BY position_id, observed_at, id
            """,
        ).fetchall()
        mcap_rows = conn.execute(
            """
            SELECT position_id, mcap_usd
            FROM candidate_log
            WHERE strategy = 'B' AND entered = TRUE AND position_id IS NOT NULL
              AND mcap_usd IS NOT NULL
            """,
        ).fetchall()
    finally:
        conn.close()

    mcap_by_position: dict[str, float] = {}
    for position_id, mcap in mcap_rows:
        current = mcap_by_position.get(str(position_id))
        if current is None or float(mcap) > current:
            mcap_by_position[str(position_id)] = float(mcap)

    snapshots: dict[str, list[tuple[str, float]]] = {}
    for position_id, observed_at, price in snapshot_rows:
        snapshots.setdefault(str(position_id), []).append((str(observed_at), float(price)))

    records: list[PositionRecord] = []
    for position_id, opened_at, entry, peak, close, amount in rows:
        opened = parse_timestamp(str(opened_at))
        points: list[tuple[float, float]] = []
        for observed_at, price in snapshots.get(str(position_id), []):
            elapsed = (parse_timestamp(observed_at) - opened).total_seconds()
            if elapsed >= 0:
                points.append((elapsed, price))
        points.sort()
        records.append(
            PositionRecord(
                id=str(position_id),
                opened_at=opened,
                entry=float(entry),
                peak=float(peak),
                close=float(close),
                amount=float(amount),
                mcap_usd=mcap_by_position.get(str(position_id)),
                snapshots=tuple(points),
            ),
        )
    return records


def simulate_position(record: PositionRecord, trail: float, tp: float, hard: float) -> float:
    """Return simulated SOL PnL, preferring ordered snapshot marks when present."""
    entry = record.entry
    peak = entry
    for elapsed, price in record.snapshots:
        peak = max(peak, price)
        if price >= entry * (1 + tp / 100):
            return record.amount * (tp / 100)
        if price <= entry * (1 - hard / 100):
            return -record.amount * (hard / 100)
        if peak > entry * (1 + TRAILING_ARM_PCT / 100) and (peak - price) / peak >= trail / 100:
            return record.amount * (price / entry - 1)
        if (
            elapsed >= ENTRY_CONFIRM_WINDOW_S
            and peak <= entry * (1 + EARLY_EXIT_GREEN_PCT / 100)
        ):
            return record.amount * (price / entry - 1)
        if elapsed >= TIME_STOP_MINUTES * 60:
            return record.amount * (price / entry - 1)

    if record.snapshots:
        return record.amount * (record.snapshots[-1][1] / entry - 1)

    # MT-520 peak-bound fallback for positions without a recorded mark path.
    if record.peak >= entry * (1 + tp / 100):
        exit_price = entry * (1 + tp / 100)
    else:
        exit_price = min(record.close, record.peak * (1 - trail / 100))
        exit_price = max(exit_price, entry * (1 - hard / 100))
    return record.amount * (exit_price / entry - 1)


def metrics(pnls: list[float]) -> dict[str, float | int | None]:
    if not pnls:
        return {
            "trades": 0, "pnl_sol": 0.0, "win_rate": 0.0, "profit_factor": None,
            "avg_win": 0.0, "avg_loss": 0.0, "pnl_per_trade": 0.0,
        }
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    profit_factor = gross_win / gross_loss if gross_loss > 0 else None
    return {
        "trades": len(pnls),
        "pnl_sol": round(sum(pnls), 8),
        "win_rate": round(len(wins) / len(pnls), 6),
        "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
        "avg_win": round(fmean(wins), 8) if wins else 0.0,
        "avg_loss": round(fmean(losses), 8) if losses else 0.0,
        "pnl_per_trade": round(sum(pnls) / len(pnls), 8),
    }


def apply_filter(records: list[PositionRecord], filter_name: str) -> list[PositionRecord]:
    if filter_name == "baseline":
        return records
    if filter_name == "no_wed":
        return [r for r in records if r.opened_at.weekday() != 2]
    if filter_name == "no_utc14":
        return [r for r in records if r.opened_at.hour != 14]
    if filter_name == "no_wed_no_utc14":
        return [r for r in records if r.opened_at.weekday() != 2 and r.opened_at.hour != 14]
    if filter_name == "thu_fri":
        return [r for r in records if r.opened_at.weekday() in (3, 4)]
    if filter_name == "golden_hours":
        return [r for r in records if r.opened_at.hour in GOLDEN_HOURS]
    raise ValueError(f"Unknown filter: {filter_name}")


def evaluate(
    records: list[PositionRecord], trail: float, tp: float, hard: float,
) -> dict[str, object]:
    return metrics([simulate_position(record, trail, tp, hard) for record in records])


def main() -> None:
    records = load_positions()
    snapshot_backed = sum(1 for record in records if record.snapshots)
    fallback = len(records) - snapshot_backed
    print(f"Loaded {len(records)} closed Strategy B positions "
          f"({snapshot_backed} snapshot-backed, {fallback} peak-bound fallback)")

    def combo_key(trail: float, tp: float, hard: float) -> dict[str, object]:
        return {"trailing_stop_pct": trail, "take_profit_pct": tp, "hard_stop_pct": hard}

    # Step 2 — full exit-parameter sweep on all trades (no filter).
    step2: list[dict[str, object]] = []
    for trail, tp, hard in product(TRAIL_VALUES, TAKE_PROFIT_VALUES, HARD_STOP_VALUES):
        result = evaluate(records, trail, tp, hard)
        result["parameters"] = combo_key(trail, tp, hard)
        result["filter"] = "baseline"
        step2.append(result)
    step2.sort(key=lambda item: float(item["pnl_sol"]), reverse=True)

    # Step 3 — top 5 parameter combos across day/hour filters.
    top5_combos = [item["parameters"] for item in step2[:5]]
    step3: list[dict[str, object]] = []
    for parameters in top5_combos:
        for filter_name, filter_label in FILTERS.items():
            filtered = apply_filter(records, filter_name)
            result = evaluate(filtered, float(parameters["trailing_stop_pct"]),
                              float(parameters["take_profit_pct"]),
                              float(parameters["hard_stop_pct"]))
            result["parameters"] = parameters
            result["filter"] = filter_name
            result["filter_label"] = filter_label
            step3.append(result)

    # Step 4 — top 3 step-3 combos restricted to $20K+ mcap trades.
    mcap_records = [
        record for record in records
        if record.mcap_usd is not None and record.mcap_usd >= MCAP_FLOOR_USD
    ]
    step3_sorted = sorted(step3, key=lambda item: float(item["pnl_sol"]), reverse=True)
    step4: list[dict[str, object]] = []
    for item in step3_sorted[:3]:
        parameters = item["parameters"]
        filtered = apply_filter(mcap_records, str(item["filter"]))
        result = evaluate(filtered, float(parameters["trailing_stop_pct"]),
                          float(parameters["take_profit_pct"]),
                          float(parameters["hard_stop_pct"]))
        result["parameters"] = parameters
        result["filter"] = item["filter"]
        result["filter_label"] = item["filter_label"]
        step4.append(result)

    # Baseline — current live parameters (4% / 80% / 10%), no filter.
    baseline_result = evaluate(records, BASELINE["trailing_stop_pct"],
                               BASELINE["take_profit_pct"], BASELINE["hard_stop_pct"])

    # Step 5 — ranked top-10 parameter + filter combinations.
    combined = [dict(item) for item in step3]
    combined.sort(key=lambda item: float(item["pnl_sol"]), reverse=True)
    top10 = combined[:10]

    results = {
        "methodology": {
            "positions_analyzed": len(records),
            "snapshot_backed": snapshot_backed,
            "peak_bound_fallback": fallback,
            "exit_order": "take_profit -> hard_stop -> trailing_stop (armed at 2%) -> "
                          "early_exit_no_green (90s, 1%) -> time_stop (10 min)",
            "fallback_model": "MT-520 peak-bound approximation using persisted entry/peak/close",
            "mcap_source": "candidate_log (entered=TRUE, max mcap per position)",
            "filters": FILTERS,
        },
        "baseline_parameters": BASELINE,
        "baseline_no_filter": baseline_result,
        "step2_full_sweep": step2,
        "step3_top5_x_filters": step3,
        "step4_top3_on_mcap_20k": {"mcap_floor_usd": MCAP_FLOOR_USD,
                                   "mcap_records": len(mcap_records), "results": step4},
        "top10_ranked": top10,
    }
    JSON_PATH.write_text(json.dumps(results, indent=2) + "\n")
    REPORT_PATH.write_text(report(results))
    print(f"Wrote {JSON_PATH}")
    print(f"Wrote {REPORT_PATH}")


def report(results: dict[str, object]) -> str:
    baseline = results["baseline_no_filter"]
    lines = [
        "# MT-552 Parameter Sweep — Strategy B Exits",
        "",
        f"Analyzed **{results['methodology']['positions_analyzed']}** closed Strategy B trades "
        f"({results['methodology']['snapshot_backed']} snapshot-backed, "
        f"{results['methodology']['peak_bound_fallback']} peak-bound fallback).",
        "",
        "## Baseline — current live parameters (4% trail / 80% TP / 10% hard stop, no filter)",
        "",
        "| Trades | PnL | WR% | PF | Avg win | Avg loss |",
        "|---:|---:|---:|---:|---:|---:|",
        f"| {baseline['trades']} | {baseline['pnl_sol']:+.4f} | {baseline['win_rate']*100:.1f} "
        f"| {baseline['profit_factor']:.2f} | {baseline['avg_win']:+.4f} | "
        f"{baseline['avg_loss']:+.4f} |",
        "",
        "## Step 2 — Full exit-parameter sweep (no filter)",
        "",
        "Grid: trail {2,3,4,5,6,8}% x TP {60,80,100,120,150}% x hard {8,10,15,20}% "
        f"({len(results['step2_full_sweep'])} combinations).",
        "",
    ]
    lines += _ranked_table(
        results["step2_full_sweep"][:10], title="Top 10 by total PnL (no filter)",
    )
    lines += ["", "## Step 3 — Top 5 parameter combos across day/hour filters", "",
              "Filters: no Wednesday / no UTC 14 / no Wed + no UTC 14 / only Thu+Fri / "
              "golden hours (UTC 4-6, 8-12, 17).", ""]
    lines += _ranked_table(
        results["step3_top5_x_filters"], title="Ranked parameter + filter combinations",
    )
    lines += ["", "## Step 4 — Top 3 step-3 combos on $20K+ mcap trades", "",
              f"Filtered to the {results['step4_top3_on_mcap_20k']['mcap_records']} trades "
              "with mcap >= $20K (the tier carrying the majority of profit).", ""]
    lines += _ranked_table(
        results["step4_top3_on_mcap_20k"]["results"], title="Top 3 on $20K+ mcap tier",
    )
    lines += ["", "## Step 5 — Overall top 10 (ranked)", ""]
    lines += _ranked_table(
        results["top10_ranked"],
        title="Top 10 parameter + filter combinations by total PnL",
    )
    lines += [
        "",
        "### Notes",
        "- PnL is simulated SOL PnL from replaying recorded price paths; the current-parameters "
        "baseline row is the comparison point.",
        "- Positions without position-linked snapshots use the MT-520 peak-bound approximation "
        "(`min(close, peak*(1-trail))` clamped to the hard stop, or TP when the persisted peak "
        "reaches it) — exit-parameter results on those trades are approximate.",
        "- Day/hour filters classify by `opened_at` (UTC).",
        "- Read-only analysis; no live parameters, code, or database rows were changed.",
    ]
    return "\n".join(lines) + "\n"


def _ranked_table(items: list[dict[str, object]], title: str) -> list[str]:
    header = [
        title, "",
        "| Rank | Trail | TP | Stop | Filter | Trades | PnL | WR% | PF | Avg win | Avg loss |",
        "|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    rows = []
    for rank, item in enumerate(items, 1):
        parameters = item["parameters"]
        filter_label = str(
            item.get("filter_label") or FILTERS.get(str(item["filter"]), str(item["filter"])),
        )
        pf = f"{item['profit_factor']:.2f}" if item["profit_factor"] is not None else "—"
        rows.append(
            f"| {rank} | {parameters['trailing_stop_pct']}% | {parameters['take_profit_pct']}% "
            f"| {parameters['hard_stop_pct']}% | {filter_label} | {item['trades']} "
            f"| {item['pnl_sol']:+.4f} | {item['win_rate']*100:.1f} | {pf} "
            f"| {item['avg_win']:+.4f} | {item['avg_loss']:+.4f} |",
        )
    return header + rows + [""]


if __name__ == "__main__":
    main()
