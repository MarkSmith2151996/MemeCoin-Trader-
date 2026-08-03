#!/usr/bin/env python3
"""Robust exit parameter grid search with walk-forward validation (MT-502).

Reads closed positions from data/trades.db (read-only) and:
  1. Runs a finer grid (trailing 4-16%, TP 15-60%, hard stop 10-25%) under
     walk-forward validation: 400-trade train / 200-trade test windows
     sliding by 200 trades.
  2. Ranks combos by average out-of-sample (test) PnL across all windows.
  3. Runs a sensitivity heatmap around the top 3 combos.
  4. Prints a recommendation with a confidence level.

Simulation logic (per trade):
  - If peak >= entry * (1 + tp): exit at TP price
  - Else: trailing stop exit at peak * (1 - trail) * (1 - slippage)
  - Floored at hard stop: entry * (1 - hard)
  - Never exits above peak

The database is never written to. Live trading parameters are untouched.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trades.db"
OUT_PATH = ROOT / "data" / "grid_search_results.json"

SLIPPAGE = 0.025

TRAIL_VALUES = list(range(4, 17))       # 4%..16% in 1% steps (13 values)
TP_VALUES = list(range(15, 61, 5))      # 15%..60% in 5% steps (10 values)
HARD_VALUES = list(range(10, 26, 5))    # 10%..25% in 5% steps (4 values)

WINDOW_TRAIN = 400
WINDOW_TEST = 200
WINDOW_SLIDE = 200
TOP_N_PER_WINDOW = 3

SENS_TRAIL_SPAN = 3   # +-3% trailing around combo
SENS_TP_SPAN = 10     # +-10% TP around combo


def load_trades() -> np.ndarray:
    """Load closed positions with a peak price, ordered by opened_at."""
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT entry_price_sol, peak_price_sol, close_price_sol, amount_sol
            FROM positions
            WHERE status='CLOSED' AND peak_price_sol IS NOT NULL
            ORDER BY opened_at
            """
        ).fetchall()
    finally:
        conn.close()
    arr = np.asarray(rows, dtype=float)
    valid = (arr[:, 0] > 0) & (arr[:, 1] > 0) & (arr[:, 3] > 0)
    return arr[valid]


def build_combos():
    """Finer grid; skip combos where trailing >= hard stop."""
    combos = []
    for trail in TRAIL_VALUES:
        for tp in TP_VALUES:
            for hard in HARD_VALUES:
                if trail >= hard:
                    continue
                combos.append((trail, tp, hard))
    return combos


def simulate(entries, peaks, amounts, trail, tp, hard):
    """Vectorized exit simulation. Percent inputs, PnL in SOL."""
    tp_hit = peaks >= entries * (1.0 + tp / 100.0)
    exit_px = np.where(
        tp_hit,
        entries * (1.0 + tp / 100.0),
        peaks * (1.0 - trail / 100.0) * (1.0 - SLIPPAGE),
    )
    exit_px = np.maximum(exit_px, entries * (1.0 - hard / 100.0))
    exit_px = np.minimum(exit_px, peaks)
    return ((exit_px / entries) - 1.0) * amounts


def run_grid(entries, peaks, amounts, combos):
    """Run the full grid over one slice. Returns {combo: total PnL SOL}."""
    results = {}
    for trail, tp, hard in combos:
        results[(trail, tp, hard)] = float(
            simulate(entries, peaks, amounts, trail, tp, hard).sum()
        )
    return results


def format_pnl(x: float) -> str:
    return f"{x:+.2f}"


def print_heatmap(combo, trail_vals, tp_vals, entries, peaks, amounts):
    trail, tp, hard = combo
    print(f"  heatmap for combo  trail={trail}%  TP={tp}%  hard={hard}%  (PnL SOL, full dataset)")
    header = "  trail\\TP |" + "".join(f"{v:>8}" for v in tp_vals)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for t in trail_vals:
        row = f"  {t:>5}%   |"
        for p in tp_vals:
            pnl = float(simulate(entries, peaks, amounts, t, p, hard).sum())
            row += f"{format_pnl(pnl):>8}"
        print(row)


def main():
    arr = load_trades()
    entries, peaks, amounts = arr[:, 0], arr[:, 1], arr[:, 3]
    n = len(arr)
    print(f"Loaded {n} closed positions from {DB_PATH}")
    print(f"Grid: {len(TRAIL_VALUES)} trailing x {len(TP_VALUES)} TP x "
          f"{len(HARD_VALUES)} hard-stop values")
    combos = build_combos()
    print(f"Combos after skipping trailing>=hard: {len(combos)}\n")

    # ---- 1. Walk-forward windows ----
    windows = []
    start = 0
    while start + WINDOW_TRAIN <= n:
        train_slice = slice(start, start + WINDOW_TRAIN)
        test_slice = slice(start + WINDOW_TRAIN, min(start + WINDOW_TRAIN + WINDOW_TEST, n))
        if (test_slice.stop - test_slice.start) < 1:
            break
        windows.append((train_slice, test_slice))
        start += WINDOW_SLIDE

    print(f"Walk-forward: {len(windows)} windows "
          f"(train {WINDOW_TRAIN}, test {WINDOW_TEST}, slide {WINDOW_SLIDE})\n")

    per_window = []  # top combos per window (by train PnL)
    all_pnl = {c: [] for c in combos}  # test PnL per combo across windows
    all_pnl_train = {c: [] for c in combos}

    for i, (train_slice, test_slice) in enumerate(windows, 1):
        n_train = train_slice.stop - train_slice.start
        n_test = test_slice.stop - test_slice.start
        train_pnl = run_grid(
            entries[train_slice], peaks[train_slice], amounts[train_slice], combos
        )
        test_pnl = run_grid(
            entries[test_slice], peaks[test_slice], amounts[test_slice], combos
        )
        for c in combos:
            all_pnl[c].append(test_pnl[c])
            all_pnl_train[c].append(train_pnl[c])

        top = sorted(train_pnl, key=train_pnl.get, reverse=True)[:TOP_N_PER_WINDOW]
        test_ranks = {c: rank for rank, c in enumerate(
            sorted(test_pnl, key=test_pnl.get, reverse=True), 1)}
        best_test = max(test_pnl, key=test_pnl.get)
        per_window.append(top)

        print(f"=== Window {i}: train rows {train_slice.start + 1}-{train_slice.stop} "
              f"({n_train}), test rows {test_slice.start + 1}-{test_slice.stop} ({n_test}) ===")
        for rank, c in enumerate(top, 1):
            t, tp, h = c
            print(f"  #{rank} train: trail={t}% TP={tp}% hard={h}%  "
                  f"train PnL {format_pnl(train_pnl[c])}  "
                  f"test PnL {format_pnl(test_pnl[c])} (test rank {test_ranks[c]})")
        t, tp, h = best_test
        print(f"  best on TEST: trail={t}% TP={tp}% hard={h}%  "
              f"test PnL {format_pnl(test_pnl[best_test])}")
        print()

    # ---- Consistency: which combos win across windows ----
    print("=== Win consistency (top-1 / top-3 by train PnL per window) ===")
    win_counts = {}
    top3_counts = {}
    for c in combos:
        win_counts[c] = sum(1 for top in per_window if top[0] == c)
        top3_counts[c] = sum(1 for top in per_window if c in top)
    consistent = [
        (c, win_counts[c], top3_counts[c])
        for c in combos
        if win_counts[c] > 0 or top3_counts[c] == len(windows)
    ]
    consistent.sort(key=lambda x: (-x[1], -x[2]))
    for c, w, t3 in consistent[:15]:
        print(f"  trail={c[0]}% TP={c[1]}% hard={c[2]}%  wins {w}/{len(windows)} windows, "
              f"top-3 in {t3}/{len(windows)}")
    print()

    # ---- 2. Overall ranking by average TEST PnL ----
    print("=== Overall ranking: average test PnL across all windows (top 15) ===")
    ranked = sorted(
        combos,
        key=lambda c: (float(np.mean(all_pnl[c])), float(np.mean(all_pnl_train[c]))),
        reverse=True,
    )
    for i, c in enumerate(ranked[:15], 1):
        print(f"  #{i:2d} trail={c[0]}% TP={c[1]}% hard={c[2]}%  "
              f"avg test {format_pnl(np.mean(all_pnl[c]))}  "
              f"(min {format_pnl(min(all_pnl[c]))}, max {format_pnl(max(all_pnl[c]))})  "
              f"avg train {format_pnl(np.mean(all_pnl_train[c]))}  "
              f"wins {win_counts[c]}/{len(windows)}")
    print()

    # ---- 3. Sensitivity heatmap for top 3 ----
    top3 = ranked[:3]
    print("=== Sensitivity analysis (full dataset PnL, hard stop fixed) ===")
    sens = {}
    for combo in top3:
        trail, tp, hard = combo
        trail_vals = [t for t in TRAIL_VALUES if abs(t - trail) <= SENS_TRAIL_SPAN]
        tp_vals = [p for p in TP_VALUES if abs(p - tp) <= SENS_TP_SPAN]
        print_heatmap(combo, trail_vals, tp_vals, entries, peaks, amounts)
        pnls = {
            (t, p, hard): float(simulate(entries, peaks, amounts, t, p, hard).sum())
            for t in trail_vals for p in tp_vals
        }
        sens[str(combo)] = {
            "trail_vals": trail_vals,
            "tp_vals": tp_vals,
            "hard": hard,
            "pnl": {f"t{t}_tp{p}": v for (t, p, _), v in pnls.items()},
            "n_positive": sum(1 for v in pnls.values() if v > 0),
            "n_total": len(pnls),
        }
        print()
    print()

    # ---- 4. Recommendation ----
    best = ranked[0]
    best_mean = float(np.mean(all_pnl[best]))
    best_wins = win_counts[best]
    best_t3 = top3_counts[best]
    sens_frac = sens[str(best)]["n_positive"] / sens[str(best)]["n_total"]

    if best_mean > 0 and best_t3 == len(windows) and sens_frac == 1.0:
        confidence = "HIGH"
    elif best_mean > 0 and best_wins >= max(1, len(windows) // 2) and sens_frac >= 0.8:
        confidence = "MEDIUM"
    elif best_mean > 0:
        confidence = "LOW"
    else:
        confidence = "LOW (no profitable combo)"

    print("=== Final recommendation ===")
    print(f"  Best combo: trail={best[0]}% TP={best[1]}% hard={best[2]}%")
    print(f"  Avg test PnL: {format_pnl(best_mean)} SOL across {len(windows)} windows")
    print(f"  Consistency: top-1 in {best_wins}/{len(windows)} windows, "
          f"top-3 in {best_t3}/{len(windows)}")
    print(f"  Sensitivity: {sens[str(best)]['n_positive']}/{sens[str(best)]['n_total']} "
          f"neighborhood combos profitable")
    print(f"  Confidence: {confidence}")

    # ---- Save results ----
    results = {
        "n_trades": n,
        "windows": len(windows),
        "grid": {
            "trailing": TRAIL_VALUES,
            "take_profit": TP_VALUES,
            "hard_stop": HARD_VALUES,
            "slippage": SLIPPAGE,
            "combos_total": len(combos),
        },
        "walk_forward": {
            "window_train": WINDOW_TRAIN,
            "window_test": WINDOW_TEST,
            "slide": WINDOW_SLIDE,
        },
        "windows_detail": [
            {
                "window": i + 1,
                "train_rows": f"{w[0].start + 1}-{w[0].stop}",
                "test_rows": f"{w[1].start + 1}-{w[1].stop}",
                "top_train": [
                    {"trail": c[0], "tp": c[1], "hard": c[2],
                     "train_pnl": all_pnl_train[c][i],
                     "test_pnl": all_pnl[c][i]}
                    for c in per_window[i]
                ],
            }
            for i, w in enumerate(windows)
        ],
        "ranking": [
            {"trail": c[0], "tp": c[1], "hard": c[2],
             "avg_test_pnl": float(np.mean(all_pnl[c])),
             "min_test_pnl": float(np.min(all_pnl[c])),
             "max_test_pnl": float(np.max(all_pnl[c])),
             "avg_train_pnl": float(np.mean(all_pnl_train[c])),
             "wins": win_counts[c],
             "top3_windows": top3_counts[c]}
            for c in ranked
        ],
        "sensitivity": sens,
        "recommendation": {
            "combo": {"trail": best[0], "tp": best[1], "hard": best[2]},
            "avg_test_pnl": best_mean,
            "wins": best_wins,
            "top3_windows": best_t3,
            "sensitivity_positive_fraction": sens_frac,
            "confidence": confidence,
        },
    }
    OUT_PATH.write_text(json.dumps(results, indent=2))
    print(f"\nFull results saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
