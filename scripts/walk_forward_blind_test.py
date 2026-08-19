"""MT-592: blind-test tuned gates on an untouched window (v2).

Applies a tuned gates JSON to a held-out window of the replay features CSV
with zero adjustment and compares tuned vs baseline (trade count, win rate,
total PnL, avg PnL/trade). Writes the comparison markdown with a pass/fail
verdict against the MT-592 success criteria — ALL four must hold:

1. Total PnL >= baseline
2. Win rate improvement >= 2pp
3. Trade retention >= 40%
4. Avg PnL/trade >= baseline

Usage:
    python3 scripts/walk_forward_blind_test.py --start 2026-06-01 --end 2026-06-30 \
        --gates data/walk_forward/iter1_gates.json \
        --output data/walk_forward/iter1_blind_test.md
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FEATURES = REPO_ROOT / "data" / "walk_forward" / "replay_features.csv"
DEFAULT_GATES = REPO_ROOT / "data" / "walk_forward" / "iter1_gates.json"
DEFAULT_OUT = REPO_ROOT / "data" / "walk_forward" / "iter1_blind_test.md"

WR_IMPROVE_PP = 2.0
RETENTION_MIN = 40.0

GATE_FEATURE = {
    "mcap_min_usd": ("mcap_usd", "min"),
    "mcap_max_usd": ("mcap_usd", "max"),
    "volume_min_usd": ("volume_usd", "min"),
    "buy_sell_min": ("buy_sell_ratio", "min"),
    "txns_min": ("txns_total", "min"),
    "age_max_minutes": ("age_minutes", "max"),
    "liquidity_min_usd": ("liquidity_usd", "min"),
    "pool_sol_min": ("pool_sol", "min"),
    "top10_holder_max": ("top10_holder_pct", "max"),
    "creator_holdings_max": ("creator_holdings_pct", "max"),
    "vol_mcap_min": ("vol_mcap_ratio", "min"),
    "vol_mcap_max": ("vol_mcap_ratio", "max"),
    "score_min": ("score_proxy", "min"),
    "score_v1_min": ("score_v1", "min"),
}


def apply_gates(frame: pd.DataFrame, gates: dict) -> pd.DataFrame:
    passed = frame.copy()
    for key, threshold in gates.items():
        feature, direction = GATE_FEATURE[key]
        values = passed[feature]
        passed = passed[values.notna() & (values >= threshold if direction == "min" else values <= threshold)]
    return passed


def metrics(frame: pd.DataFrame) -> dict:
    total = len(frame)
    if total == 0:
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "total_pnl_sol": 0.0, "avg_pnl_sol": 0.0}
    wins = int((frame["win"] > 0).sum())
    pnl = float(frame["pnl_sol"].sum())
    return {
        "trades": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": wins / total * 100.0 if total else 0.0,
        "total_pnl_sol": round(pnl, 6),
        "avg_pnl_sol": round(pnl / total, 6) if total else 0.0,
    }


def tier_breakdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "  (empty cohort)"
    def bucket(mcap: float) -> str:
        if mcap < 10_000:
            return "$5-10K"
        if mcap < 20_000:
            return "$10-20K"
        return "$20-50K"
    frame = frame.copy()
    frame["tier"] = frame["mcap_usd"].apply(bucket)
    lines = ["  | mcap tier | trades | wins | win rate | PnL (SOL) |", "  |---|---|---:|---:|---:|"]
    for tier, group in frame.groupby("tier", sort=False):
        group = group.dropna(subset=["pnl_sol"])
        lines.append(
            f"  | {tier} | {len(group)} | {int((group['win'] > 0).sum())} | "
            f"{int((group['win'] > 0).sum()) / len(group) * 100:.1f}% | {group['pnl_sol'].sum():+.4f} |"
        )
    return "\n".join(lines)


def exit_breakdown(frame: pd.DataFrame) -> str:
    counts = frame["exit_reason"].value_counts()
    lines = ["  | exit | count |", "  |---|---:|"]
    for reason, count in counts.items():
        lines.append(f"  | {reason} | {count} |")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--start", help="Blind window start YYYY-MM-DD (entry_date filter).")
    parser.add_argument("--end", help="Blind window end YYYY-MM-DD (inclusive).")
    parser.add_argument("--gates", type=Path, default=DEFAULT_GATES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--title", default="Blind Test", help="Markdown title for the report.")
    parser.add_argument("--window-label", default="", help="Window label shown in the report.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = pd.read_csv(args.features)
    if "win" not in frame.columns:
        frame["win"] = (frame["pnl_sol"] > 0).astype(int)
    if args.start:
        frame = frame[frame["entry_date"] >= args.start]
    if args.end:
        frame = frame[frame["entry_date"] <= args.end]
    if frame.empty:
        raise SystemExit(f"No rows in {args.features} for window {args.start}..{args.end}")

    payload = json.loads(args.gates.read_text(encoding="utf-8"))
    gates = payload["gates"]
    window_label = args.window_label or f"{args.start or '?'}..{args.end or '?'}"

    actual = metrics(frame)
    tuned = metrics(apply_gates(frame, gates))
    removed = frame[~frame.index.isin(apply_gates(frame, gates).index)]

    pnl_delta = tuned["total_pnl_sol"] - actual["total_pnl_sol"]
    wr_delta = tuned["win_rate"] - actual["win_rate"]
    retention = tuned["trades"] / actual["trades"] * 100.0 if actual["trades"] else 0.0
    avg_delta = tuned["avg_pnl_sol"] - actual["avg_pnl_sol"]

    checks = {
        "total PnL >= baseline": tuned["total_pnl_sol"] >= actual["total_pnl_sol"],
        "win rate improved >= 2pp": wr_delta >= WR_IMPROVE_PP,
        "retained >= 40% of trades": retention >= RETENTION_MIN,
        "avg PnL/trade >= baseline": tuned["avg_pnl_sol"] >= actual["avg_pnl_sol"],
    }
    passed = all(checks.values())

    lines = [
        f"# {args.title}",
        "",
        f"Blind window: **{window_label}**",
        "",
        f"Tuned gates applied with zero adjustment: `{json.dumps(gates)}`",
        "",
        "## Baseline (replay, MT-569 friction)",
        "",
        f"- Trades: **{actual['trades']}**",
        f"- Win rate: **{actual['win_rate']:.1f}%** ({actual['wins']} wins / {actual['losses']} losses)",
        f"- Total PnL: **{actual['total_pnl_sol']:+.6f} SOL**",
        f"- Avg PnL per trade: {actual['avg_pnl_sol']:+.6f} SOL",
        "",
        "## Tuned gates (applied blind, no adjustment)",
        "",
        f"- Trades: **{tuned['trades']}** ({retention:.1f}% retention)",
        f"- Win rate: **{tuned['win_rate']:.1f}%** ({tuned['wins']} wins / {tuned['losses']} losses)",
        f"- Total PnL: **{tuned['total_pnl_sol']:+.6f} SOL**",
        f"- Avg PnL per trade: {tuned['avg_pnl_sol']:+.6f} SOL",
        "",
        "## Comparison",
        "",
        "| metric | baseline | tuned | delta |",
        "|---|---:|---:|---:|",
        f"| trades | {actual['trades']} | {tuned['trades']} | {tuned['trades'] - actual['trades']:+d} ({retention:.0f}% retained) |",
        f"| win rate | {actual['win_rate']:.1f}% | {tuned['win_rate']:.1f}% | {wr_delta:+.1f}pp |",
        f"| total PnL | {actual['total_pnl_sol']:+.6f} SOL | {tuned['total_pnl_sol']:+.6f} SOL | {pnl_delta:+.6f} SOL |",
        f"| avg PnL/trade | {actual['avg_pnl_sol']:+.6f} SOL | {tuned['avg_pnl_sol']:+.6f} SOL | {avg_delta:+.6f} SOL |",
        "",
        "## Verdict checks (all four must hold)",
        "",
        "| check | result |",
        "|---|---|",
    ]
    for label, ok in checks.items():
        lines.append(f"| {label} | {'PASS' if ok else 'FAIL'} |")
    lines += [
        "",
        f"## Verdict: **{'PASS' if passed else 'FAIL'}**",
        "",
        "### Removed cohort (baseline trades the tuned gates excluded)",
        "",
        f"Removed {len(removed)} trades: WR {metrics(removed)['win_rate']:.1f}%, "
        f"PnL {metrics(removed)['total_pnl_sol']:+.6f} SOL, avg {metrics(removed)['avg_pnl_sol']:+.6f} SOL/trade",
        "",
        "Tier breakdown of the removed cohort:",
        "",
        f"{tier_breakdown(removed)}",
        "",
        "Exit breakdown of the removed cohort:",
        "",
        f"{exit_breakdown(removed)}",
        "",
        "### Tuned cohort exit breakdown",
        "",
        f"{exit_breakdown(apply_gates(frame, gates))}",
        "",
    ]
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
