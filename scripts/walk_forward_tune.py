"""MT-591 step 2: train simple gate thresholds on week 1 features.

For every numeric gate variable, scan thresholds and find the one that best
separates winners from losers on the training window (maximize passing-trade
total PnL, subject to a minimum cohort size and a win-rate floor). Then run a
greedy forward selection that adds the best remaining gate until no further
improvement. Output is the tuned gate config plus the per-gate search table.

Deliberately simple and interpretable — single-variable decision boundaries
only, no ML model. Strictly trains on the week 1 CSV only.

Usage:
    python3 scripts/walk_forward_tune.py
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_IN = REPO_ROOT / "data" / "walk_forward" / "week1_features.csv"
DEFAULT_OUT = REPO_ROOT / "data" / "walk_forward" / "tuned_gates_v1.json"

MIN_COHORT = 50
WR_FLOOR_DELTA = 5.0
MIN_IMPROVEMENT = 0.02
MAX_GATES = 5


@dataclass
class GateSpec:
    key: str
    feature: str
    direction: str  # "min" means pass when feature >= threshold; "max" when <=
    round_to: float

    def apply(self, frame: pd.DataFrame, threshold: float) -> pd.Series:
        values = frame[self.feature]
        if self.direction == "min":
            return values >= threshold
        return values <= threshold


GATE_SPECS: list[GateSpec] = [
    GateSpec("mcap_min_usd", "mcap_usd", "min", 100.0),
    GateSpec("mcap_max_usd", "mcap_usd", "max", 100.0),
    GateSpec("volume_min_usd", "volume_usd", "min", 50.0),
    GateSpec("buy_sell_min", "buy_sell_ratio", "min", 0.05),
    GateSpec("txns_min", "txns_total", "min", 1.0),
    GateSpec("age_max_minutes", "age_minutes", "max", 0.5),
    GateSpec("liquidity_min_usd", "liquidity_usd", "min", 1000.0),
    GateSpec("top10_holder_max", "top10_holder_pct", "max", 1.0),
    GateSpec("vol_mcap_min", "vol_mcap_ratio", "min", 0.005),
    GateSpec("vol_mcap_max", "vol_mcap_ratio", "max", 0.005),
    GateSpec("score_min", "score_proxy", "min", 1.0),
]


def quantile_grid(frame: pd.DataFrame, feature: str) -> list[float]:
    values = frame[feature].dropna()
    if values.empty:
        return []
    return sorted(set(float(value) for value in np.quantile(values, np.arange(0.10, 0.96, 0.05))))


def metrics(frame: pd.DataFrame) -> dict:
    total = len(frame)
    wins = int((frame["win"] > 0).sum())
    pnl = float(frame["realized_pnl_sol"].sum())
    return {
        "trades": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": wins / total * 100.0 if total else 0.0,
        "total_pnl_sol": round(pnl, 6),
        "avg_pnl_sol": round(pnl / total, 6) if total else 0.0,
    }


def evaluate_gate(frame: pd.DataFrame, gate: GateSpec, threshold: float) -> dict:
    passed = frame[gate.apply(frame, threshold) & frame[gate.feature].notna()]
    passed = passed[passed["win"].notna()]
    result = metrics(passed)
    result["threshold"] = threshold
    result["retained"] = len(passed) / len(frame) * 100.0 if len(frame) else 0.0
    return result


def best_threshold(frame: pd.DataFrame, gate: GateSpec, baseline: dict) -> dict | None:
    best: dict | None = None
    for threshold in quantile_grid(frame, gate.feature):
        result = evaluate_gate(frame, gate, threshold)
        if result["trades"] < MIN_COHORT:
            continue
        if result["win_rate"] < baseline["win_rate"] - WR_FLOOR_DELTA:
            continue
        if result["total_pnl_sol"] <= baseline["total_pnl_sol"]:
            continue
        if best is None or result["total_pnl_sol"] > best["total_pnl_sol"]:
            best = result
    if best is not None:
        best["gate"] = gate.key
        best["feature"] = gate.feature
        best["direction"] = gate.direction
    return best


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_IN)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    frame = pd.read_csv(args.input)
    baseline = metrics(frame)
    per_gate: list[dict] = []

    for gate in GATE_SPECS:
        best = best_threshold(frame, gate, baseline)
        if best is not None:
            best["lift"] = best["total_pnl_sol"] / baseline["total_pnl_sol"] if baseline["total_pnl_sol"] else 0.0
            per_gate.append(best)

    per_gate.sort(key=lambda item: item["total_pnl_sol"], reverse=True)

    current = frame.copy()
    current_metrics = metrics(current)
    selected: list[dict] = []
    remaining = list(GATE_SPECS)
    for _ in range(MAX_GATES):
        best: dict | None = None
        for gate in remaining:
            candidate = best_threshold(current, gate, current_metrics)
            if candidate is None:
                continue
            if candidate["total_pnl_sol"] <= current_metrics["total_pnl_sol"] * (1 + MIN_IMPROVEMENT):
                continue
            if best is None or candidate["total_pnl_sol"] > best["total_pnl_sol"]:
                best = candidate
                best["gate_spec"] = gate
        if best is None:
            break
        gate: GateSpec = best["gate_spec"]
        current = current[gate.apply(current, best["threshold"]) & current[gate.feature].notna()]
        current = current[current["win"].notna()]
        current_metrics = metrics(current)
        selected.append({k: v for k, v in best.items() if k != "gate_spec"})
        remaining.remove(gate)

    gates: dict[str, float] = {}
    for item in selected:
        gates[item["gate"]] = item["threshold"]

    payload = {
        "meta": {
            "task": "MT-591",
            "version": "v1",
            "window": "2026-08-05..2026-08-11",
            "method": "single-variable threshold scan + greedy forward selection, PnL objective",
            "min_cohort": MIN_COHORT,
            "max_gates": MAX_GATES,
            "created_at": datetime.now(UTC).isoformat(),
        },
        "baseline": baseline,
        "train_eval": {
            "tuned": current_metrics,
            "improvement_pnl_sol": round(current_metrics["total_pnl_sol"] - baseline["total_pnl_sol"], 6),
            "retention_pct": round(len(current) / len(frame) * 100.0, 1),
        },
        "gates": gates,
        "selection_order": [item["gate"] for item in selected],
        "gate_search": per_gate,
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"Baseline (train, {baseline['trades']} trades): WR {baseline['win_rate']:.1f}%  PnL {baseline['total_pnl_sol']:+.6f} SOL")
    print("\nPer-gate best thresholds (must beat baseline on train):")
    for item in per_gate:
        print(f"  {item['gate']:<22} {item['direction']:>3} {item['threshold']:>12,.1f} "
              f"-> {item['trades']:>4} trades  WR {item['win_rate']:.1f}%  PnL {item['total_pnl_sol']:+.6f}  lift {item['lift']:.2f}x")
    print("\nSelected gates (greedy forward selection):")
    for item in selected:
        print(f"  {item['gate']:<22} {item['direction']:>3} {item['threshold']:>12,.1f} "
              f"-> {item['trades']:>4} trades  WR {item['win_rate']:.1f}%  PnL {item['total_pnl_sol']:+.6f}")
    print(f"\nTuned train eval: {current_metrics['trades']} trades, WR {current_metrics['win_rate']:.1f}%, "
          f"PnL {current_metrics['total_pnl_sol']:+.6f} SOL (baseline {baseline['total_pnl_sol']:+.6f})")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
