"""MT-592: train gate thresholds on walk-forward training windows (v2).

Applies the five fixes from the MT-591 iteration report:

1. **Larger training window** — caller passes a multi-week/month window via
   ``--start``/``--end`` on the replay features CSV (2 months per iteration).
2. **Stability check** — every candidate threshold must improve PnL on BOTH
   halves of the training window independently, not just pooled
   (``--stability total``, the MT-591 report's literal criterion; ``avg`` is
   the avg-PnL/trade fallback).
3. **Tier-aware gating** — a candidate gate may not hard-cut through a mcap or
   pool tier that is profitable in both halves: each such tier must retain
   >= 60% of its trades.
4. **Weighted objective** — maximize a score of 0.5 x avg PnL/trade lift, 0.3
   x profit-factor lift and 0.2 x total-PnL preservation, with a minimum
   cohort of >= 15% of training and a cumulative retention floor of 60% so
   tuning never filters more aggressively than the blind-test criterion.
5. **MT-569 friction** — the input CSV carries friction-adjusted PnL from the
   replay extractor, so PnL here is comparable to the MT-569 baseline.

Output is the tuned gate config plus per-gate search table, stability and
tier diagnostics. Single-variable decision boundaries only — no ML model.

Usage:
    python3 scripts/walk_forward_tune.py --start 2026-04-18 --end 2026-05-31 \
        --output data/walk_forward/iter1_gates.json
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
DEFAULT_IN = REPO_ROOT / "data" / "walk_forward" / "replay_features.csv"
DEFAULT_OUT = REPO_ROOT / "data" / "walk_forward" / "tuned_gates_v2.json"

MIN_COHORT = 50
MIN_COHORT_FRAC = 0.15
WR_FLOOR_DELTA = 5.0
MIN_IMPROVEMENT = 0.02
MAX_GATES = 3
RETENTION_FLOOR = 0.60
TIER_RETAIN_MIN = 0.60
OBJ_AVG_WEIGHT = 0.5
OBJ_PF_WEIGHT = 0.3
OBJ_PNL_WEIGHT = 0.2

MCAP_TIERS = [(5_000, 10_000), (10_000, 20_000), (20_000, 50_000)]
POOL_TIERS = [(0.0, 10.0), (10.0, 30.0), (30.0, float("inf"))]


@dataclass
class GateSpec:
    key: str
    feature: str
    direction: str  # "min" means pass when feature >= threshold; "max" when <=
    round_to: float
    description: str

    def apply(self, frame: pd.DataFrame, threshold: float) -> pd.Series:
        values = frame[self.feature]
        if self.direction == "min":
            return values >= threshold
        return values <= threshold


GATE_SPECS: list[GateSpec] = [
    GateSpec("mcap_min_usd", "mcap_usd", "min", 100.0, "minimum market cap"),
    GateSpec("mcap_max_usd", "mcap_usd", "max", 100.0, "maximum market cap"),
    GateSpec("volume_min_usd", "volume_usd", "min", 50.0, "minimum cumulative volume (USD)"),
    GateSpec("buy_sell_min", "buy_sell_ratio", "min", 0.05, "minimum buy/sell ratio"),
    GateSpec("txns_min", "txns_total", "min", 1.0, "minimum cumulative trade count"),
    GateSpec("age_max_minutes", "age_minutes", "max", 0.5, "maximum token age at scan"),
    GateSpec("liquidity_min_usd", "liquidity_usd", "min", 1000.0, "minimum liquidity (USD; paper source)"),
    GateSpec("pool_sol_min", "pool_sol", "min", 1.0, "minimum pool depth (SOL; replay source)"),
    GateSpec("top10_holder_max", "top10_holder_pct", "max", 1.0, "maximum top-10 holder concentration"),
    GateSpec("creator_holdings_max", "creator_holdings_pct", "max", 1.0, "maximum creator holdings (replay source)"),
    GateSpec("vol_mcap_min", "vol_mcap_ratio", "min", 0.005, "minimum volume/mcap ratio"),
    GateSpec("vol_mcap_max", "vol_mcap_ratio", "max", 0.005, "maximum volume/mcap ratio"),
    GateSpec("score_min", "score_proxy", "min", 1.0, "minimum strength score (score_proxy)"),
    GateSpec("score_v1_min", "score_v1", "min", 0.05, "minimum archive score (score_v1)"),
]


def metrics(frame: pd.DataFrame) -> dict:
    total = len(frame)
    if total == 0:
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "total_pnl_sol": 0.0,
                "avg_pnl_sol": 0.0, "profit_factor": 0.0}
    wins = int((frame["win"] > 0).sum())
    losses = total - wins
    pnl = float(frame["pnl_sol"].sum())
    win_pnl = float(frame.loc[frame["pnl_sol"] > 0, "pnl_sol"].sum())
    loss_pnl = abs(float(frame.loc[frame["pnl_sol"] <= 0, "pnl_sol"].sum()))
    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total * 100.0,
        "total_pnl_sol": round(pnl, 6),
        "avg_pnl_sol": round(pnl / total, 6),
        "profit_factor": round(win_pnl / loss_pnl, 4) if loss_pnl > 0 else (99.0 if win_pnl > 0 else 0.0),
    }


def add_halves(frame: pd.DataFrame) -> pd.DataFrame:
    ordered = frame.sort_values("entry_date", kind="stable")
    split = len(ordered) // 2
    ordered = ordered.copy()
    ordered["half"] = ["first"] * split + ["second"] * (len(ordered) - split)
    return ordered


def tier_preserved(frame: pd.DataFrame, gate: GateSpec, threshold: float, tier_col: str, tier_bounds: list[tuple[float, float]]) -> bool:
    """Gate may not cut a tier that is profitable in BOTH halves below 60% retention."""
    passed = frame[gate.apply(frame, threshold) & frame[gate.feature].notna()]
    passed = passed[passed["win"].notna()]
    for lower, upper in tier_bounds:
        if upper == float("inf"):
            in_tier = frame[tier_col] >= lower
        else:
            in_tier = (frame[tier_col] >= lower) & (frame[tier_col] < upper)
        tier = frame[in_tier]
        if len(tier) < MIN_COHORT:
            continue
        first = tier[tier["half"] == "first"]["pnl_sol"].sum()
        second = tier[tier["half"] == "second"]["pnl_sol"].sum()
        if first > 0 and second > 0:
            kept = len(passed[passed.index.isin(tier.index)])
            if kept / len(tier) < TIER_RETAIN_MIN:
                return False
    return True


def evaluate_gate(frame: pd.DataFrame, gate: GateSpec, threshold: float, baseline: dict, baseline_halves: dict, stability: bool = True, objective_mode: str = "score", stability_metric: str = "total") -> dict | None:
    passed = frame[gate.apply(frame, threshold) & frame[gate.feature].notna()]
    passed = passed[passed["win"].notna()]
    result = metrics(passed)

    if result["trades"] < max(MIN_COHORT, int(MIN_COHORT_FRAC * len(frame))):
        return None
    if result["win_rate"] < baseline["win_rate"] - WR_FLOOR_DELTA:
        return None
    if stability:
        # Stability = the gate must beat the half baseline in BOTH halves
        # independently (regime-inversion guard). 'total' is the MT-591
        # report's literal criterion (improve total PnL on both halves);
        # 'avg' uses avg PnL/trade and is only a fallback for regimes where
        # the total-PnL criterion empties the search.
        for half in ("first", "second"):
            half_base = baseline_halves[half]
            half_frame = passed[passed["half"] == half]
            if stability_metric == "total":
                if half_frame["pnl_sol"].sum() <= half_base["total_pnl_sol"]:
                    return None
            else:
                if half_frame["pnl_sol"].sum() / len(half_frame) <= half_base["avg_pnl_sol"]:
                    return None
    if not tier_preserved(frame, gate, threshold, "mcap_usd", MCAP_TIERS):
        return None
    if "pool_sol" in frame.columns:
        if not tier_preserved(frame, gate, threshold, "pool_sol", POOL_TIERS):
            return None

    result["threshold"] = round(threshold, 6)
    result["retained"] = len(passed) / len(frame) * 100.0 if len(frame) else 0.0

    if objective_mode == "checks":
        # v2.2: the four blind-test criteria applied to the train window
        checks = {
            "total_pnl_ge": result["total_pnl_sol"] >= baseline["total_pnl_sol"],
            "win_rate_pp": result["win_rate"] >= baseline["win_rate"] + 2.0,
            "retention": result["retained"] >= 40.0,
            "avg_ge": result["avg_pnl_sol"] >= baseline["avg_pnl_sol"],
        }
        result["checks"] = checks
        if not all(checks.values()):
            return None
        result["score"] = round(result["total_pnl_sol"] - baseline["total_pnl_sol"], 6)
        return result

    avg_lift = result["avg_pnl_sol"] / baseline["avg_pnl_sol"] - 1.0 if baseline["avg_pnl_sol"] else 0.0
    pf_lift = result["profit_factor"] / baseline["profit_factor"] - 1.0 if baseline["profit_factor"] else 0.0
    pnl_preserve = max(-1.0, min(1.0, result["total_pnl_sol"] / baseline["total_pnl_sol"] - 1.0)) if baseline["total_pnl_sol"] else 0.0
    result["avg_lift"] = round(avg_lift, 4)
    result["pf_lift"] = round(pf_lift, 4)
    result["pnl_preserve"] = round(pnl_preserve, 4)
    result["score"] = round(OBJ_AVG_WEIGHT * avg_lift + OBJ_PF_WEIGHT * pf_lift + OBJ_PNL_WEIGHT * pnl_preserve, 4)
    return result


def quantile_grid(frame: pd.DataFrame, feature: str) -> list[float]:
    values = frame[feature].dropna()
    if values.empty:
        return []
    # include the bottom tail (1-7%) — for min-direction gates the profitable
    # cut often lives below the 10th percentile (e.g. pool ~9.6 SOL vs a 10.7
    # 10th percentile)
    levels = [0.01, 0.02, 0.03, 0.05, 0.07] + list(np.arange(0.10, 0.96, 0.05))
    return sorted(set(float(value) for value in np.quantile(values, levels)))


def best_threshold(frame: pd.DataFrame, gate: GateSpec, baseline: dict, baseline_halves: dict, objective_mode: str = "score", stability_metric: str = "total") -> dict | None:
    best: dict | None = None
    for threshold in quantile_grid(frame, gate.feature):
        result = evaluate_gate(frame, gate, threshold, baseline, baseline_halves, objective_mode=objective_mode, stability_metric=stability_metric)
        if result is None:
            continue
        if best is None or (result["score"], result["retained"]) > (best["score"], best["retained"]):
            best = result
    if best is not None:
        best["gate"] = gate.key
        best["feature"] = gate.feature
        best["direction"] = gate.direction
    return best


def available_specs(frame: pd.DataFrame) -> list[GateSpec]:
    return [gate for gate in GATE_SPECS if gate.feature in frame.columns]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_IN)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", help="Train window start YYYY-MM-DD (entry_date filter).")
    parser.add_argument("--end", help="Train window end YYYY-MM-DD (inclusive).")
    parser.add_argument("--iter-label", default="iter1", help="Iteration label written into the JSON meta.")
    parser.add_argument("--window-label", default="", help="Human-readable window description.")
    parser.add_argument(
        "--objective-mode",
        choices=("score", "checks"),
        default="score",
        help="'score' = weighted avg/PF/PnL-preservation score (v2.1); "
             "'checks' = candidate must satisfy the four blind-test criteria on train (v2.2).",
    )
    parser.add_argument(
        "--stability",
        choices=("total", "avg"),
        default="total",
        help="Stability metric per train half: 'total' = cohort total PnL must beat the half baseline "
             "(the MT-591 report's literal 'improve PnL on both halves'); 'avg' = cohort avg PnL/trade.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = pd.read_csv(args.input)
    if "pnl_sol" not in frame.columns and "realized_pnl_sol" in frame.columns:
        frame = frame.rename(columns={"realized_pnl_sol": "pnl_sol"})
    frame["win"] = (frame["pnl_sol"] > 0).astype(int)
    if args.start:
        frame = frame[frame["entry_date"] >= args.start]
    if args.end:
        frame = frame[frame["entry_date"] <= args.end]
    if frame.empty:
        raise SystemExit(f"No rows in {args.input} for window {args.start}..{args.end}")

    frame = add_halves(frame)
    baseline = metrics(frame)
    baseline_halves = {
        "first": metrics(frame[frame["half"] == "first"]),
        "second": metrics(frame[frame["half"] == "second"]),
    }
    specs = available_specs(frame)

    per_gate: list[dict] = []
    for gate in specs:
        best = best_threshold(frame, gate, baseline, baseline_halves, objective_mode=args.objective_mode, stability_metric=args.stability)
        if best is not None:
            per_gate.append(best)
    per_gate.sort(key=lambda item: item["score"], reverse=True)

    current = frame.copy()
    current_metrics = metrics(current)
    current_halves = {
        "first": metrics(current[current["half"] == "first"]),
        "second": metrics(current[current["half"] == "second"]),
    }
    selected: list[dict] = []
    remaining = list(specs)
    for _ in range(MAX_GATES):
        if len(current) / len(frame) < RETENTION_FLOOR:
            break
        best: dict | None = None
        for gate in remaining:
            if args.objective_mode == "checks":
                # v2.2: criteria are judged against the ORIGINAL window baseline
                # (the blind test compares tuned vs the full baseline window)
                candidate = best_threshold(current, gate, baseline, baseline_halves, objective_mode="checks", stability_metric=args.stability)
                if candidate is None or candidate["score"] <= 0.0:
                    continue
            else:
                candidate = best_threshold(current, gate, current_metrics, current_halves, objective_mode="score", stability_metric=args.stability)
                if candidate is None or candidate["score"] < MIN_IMPROVEMENT:
                    continue
            # never let the cumulative cohort fall below the retention floor
            retained_after = len(current) * (candidate["retained"] / 100.0) / len(frame)
            if retained_after < RETENTION_FLOOR:
                continue
            if best is None or candidate["score"] > best["score"]:
                best = candidate
                best["gate_spec"] = gate
        if best is None:
            break
        gate: GateSpec = best["gate_spec"]
        current = current[gate.apply(current, best["threshold"]) & current[gate.feature].notna()]
        current = current[current["win"].notna()]
        current_metrics = metrics(current)
        current_halves = {
            "first": metrics(current[current["half"] == "first"]),
            "second": metrics(current[current["half"] == "second"]),
        }
        selected.append({k: v for k, v in best.items() if k != "gate_spec"})
        remaining.remove(gate)

    gates: dict[str, float] = {}
    for item in selected:
        gates[item["gate"]] = item["threshold"]

    payload = {
        "meta": {
            "task": "MT-592",
            "iteration": args.iter_label,
            "window": args.window_label or f"{args.start or 'archive'}..{args.end or 'archive'}",
            "window_start": args.start,
            "window_end": args.end,
        "method": (
            "single-variable threshold scan + greedy forward selection; stability (avg PnL/trade per half), tier preservation, "
            + (
                f"four blind-test criteria as train objective (v2.2), retention floor {RETENTION_FLOOR:.0%}, max {MAX_GATES} gates"
                if args.objective_mode == "checks"
                else f"{OBJ_AVG_WEIGHT} x avg-PnL + {OBJ_PF_WEIGHT} x PF + {OBJ_PNL_WEIGHT} x PnL-preservation objective, "
                     f"cohort >= {MIN_COHORT_FRAC:.0%} of train, retention floor {RETENTION_FLOOR:.0%}, max {MAX_GATES} gates"
            )
        ),
            "min_cohort": MIN_COHORT,
            "min_cohort_frac": MIN_COHORT_FRAC,
            "tier_retain_min": TIER_RETAIN_MIN,
            "max_gates": MAX_GATES,
            "objective_mode": args.objective_mode,
            "stability": args.stability,
            "created_at": datetime.now(UTC).isoformat(),
        },
        "baseline": baseline,
        "baseline_halves": baseline_halves,
        "train_eval": {
            "tuned": current_metrics,
            "tuned_halves": current_halves,
            "baseline": baseline,
            "improvement_pnl_sol": round(current_metrics["total_pnl_sol"] - baseline["total_pnl_sol"], 6),
            "improvement_avg_pnl_sol": round(current_metrics["avg_pnl_sol"] - baseline["avg_pnl_sol"], 6),
            "retention_pct": round(len(current) / len(frame) * 100.0, 1),
        },
        "gates": gates,
        "selection_order": [item["gate"] for item in selected],
        "gate_search": per_gate,
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"Train window: {args.window_label or f'{args.start}..{args.end}'} — {baseline['trades']} trades "
          f"(halves {baseline_halves['first']['trades']}/{baseline_halves['second']['trades']})")
    print(f"Baseline: WR {baseline['win_rate']:.1f}%  PnL {baseline['total_pnl_sol']:+.6f} SOL  "
          f"avg {baseline['avg_pnl_sol']:+.6f}  PF {baseline['profit_factor']}")
    print("\nPer-gate best candidates (must pass stability + tier checks):")
    for item in per_gate:
        print(f"  {item['gate']:<22} {item['direction']:>3} {item['threshold']:>12,.3f} "
              f"-> {item['trades']:>5} trades  WR {item['win_rate']:.1f}%  PnL {item['total_pnl_sol']:+.6f}  "
              f"score {item['score']:+.3f}")
    print("\nSelected gates (greedy forward selection, score >= +0.02):")
    for item in selected:
        print(f"  {item['gate']:<22} {item['direction']:>3} {item['threshold']:>12,.3f} "
              f"-> {item['trades']:>5} trades  WR {item['win_rate']:.1f}%  PnL {item['total_pnl_sol']:+.6f}  "
              f"score {item['score']:+.3f}")
    print(f"\nTuned train eval: {current_metrics['trades']} trades ({current_metrics['trades'] / len(frame) * 100:.1f}% retention), "
          f"WR {current_metrics['win_rate']:.1f}%, PnL {current_metrics['total_pnl_sol']:+.6f} SOL "
          f"({current_metrics['total_pnl_sol'] - baseline['total_pnl_sol']:+.6f} vs baseline)")
    print(f"Half stability: first {current_halves['first']['total_pnl_sol']:+.6f} (base {baseline_halves['first']['total_pnl_sol']:+.6f}) / "
          f"second {current_halves['second']['total_pnl_sol']:+.6f} (base {baseline_halves['second']['total_pnl_sol']:+.6f})")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
