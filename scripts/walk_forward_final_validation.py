"""MT-592 final validation: apply the most recent tuned gates to paper data.

Runs only when >= 2 of 3 blind-test iterations passed. Applies the iteration-3
gates to the held-out paper trading data (CLOSED positions 2026-08-05..08-18)
and compares tuned vs actual paper results with the same four MT-592 success
criteria used in the blind tests.

Replay features do not map 1:1 to paper features; the mapping table documents
each gate's paper equivalent. ``pool_sol_min`` is converted to a USD liquidity
floor using the per-day SOL/USD price from the replay derived dir. Gates
without a usable paper column (e.g. score_v1) or with a feature that is
effectively unpopulated in the paper era are skipped and reported.

Usage:
    python3 scripts/walk_forward_final_validation.py
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GATES = REPO_ROOT / "data" / "walk_forward" / "iter3_gates.json"
DEFAULT_FEATURES = REPO_ROOT / "data" / "walk_forward" / "paper_holdout_features.csv"
DEFAULT_SOL_PRICES = Path(r"/mnt/d/pumpapi-replay/derived/sol_prices.csv")
DEFAULT_OUT = REPO_ROOT / "data" / "walk_forward" / "final_validation.md"

WR_IMPROVE_PP = 2.0
RETENTION_MIN = 40.0

# replay gate -> (paper feature, direction) mapping
PAPER_MAP = {
    "mcap_min_usd": ("mcap_usd", "min"),
    "mcap_max_usd": ("mcap_usd", "max"),
    "volume_min_usd": ("volume_usd", "min"),
    "buy_sell_min": ("buy_sell_ratio", "min"),
    "txns_min": ("txns_total", "min"),
    "age_max_minutes": ("age_minutes", "max"),
    "liquidity_min_usd": ("liquidity_usd", "min"),
    "top10_holder_max": ("top10_holder_pct", "max"),
    "vol_mcap_min": ("vol_mcap_ratio", "min"),
    "vol_mcap_max": ("vol_mcap_ratio", "max"),
    "score_min": ("score_proxy", "min"),
}

# gates whose replay feature has no usable paper analog
NOT_MAPPABLE = {
    "score_v1_min": "score_v1 is computed from replay-only fields (rate x unique traders); no paper analog",
    "creator_holdings_max": "creator_holdings_pct has no paper analog (dev_holdings_pct is 0% populated in this era)",
}


def metrics(frame: pd.DataFrame) -> dict:
    total = len(frame)
    if total == 0:
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "total_pnl_sol": 0.0, "avg_pnl_sol": 0.0}
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


def load_sol_prices(path: Path) -> dict[str, float]:
    prices: dict[str, float] = {}
    if not path.exists():
        return prices
    with path.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            try:
                price = float(row.get("sol_usd") or 0)
            except ValueError:
                continue
            if row.get("date") and price > 0:
                prices[row["date"][:10]] = price
    return prices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gates", type=Path, default=DEFAULT_GATES)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--sol-prices", type=Path, default=DEFAULT_SOL_PRICES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.gates.read_text(encoding="utf-8"))
    gates: dict[str, float] = payload["gates"]
    frame = pd.read_csv(args.features)
    frame["win"] = (frame["realized_pnl_sol"] > 0).astype(int)
    frame["entry_date"] = frame["opened_at"].astype(str).str[:10]

    sol_prices = load_sol_prices(args.sol_prices)
    if "pool_sol_min" in gates:
        frame["pool_sol_est"] = frame.apply(
            lambda row: row["liquidity_usd"] / sol_prices[row["entry_date"]]
            if pd.notna(row["liquidity_usd"]) and row["entry_date"] in sol_prices else None,
            axis=1,
        )
        populated = frame["pool_sol_est"].notna().sum()
        if populated / len(frame) >= 0.10:
            PAPER_MAP["pool_sol_min"] = ("pool_sol_est", "min")
        else:
            NOT_MAPPABLE["pool_sol_min"] = (
                f"liquidity_usd/SOL price covers only {populated}/{len(frame)} rows in the paper era"
            )

    applied: dict[str, float] = {}
    skipped: dict[str, str] = {}
    for key, threshold in gates.items():
        if key in NOT_MAPPABLE:
            skipped[key] = NOT_MAPPABLE[key]
            continue
        feature, _direction = PAPER_MAP[key]
        if feature not in frame.columns:
            skipped[key] = f"paper features CSV has no '{feature}' column"
            continue
        populated = frame[feature].notna().sum()
        if populated / len(frame) < 0.10:
            skipped[key] = f"'{feature}' populated on only {populated}/{len(frame)} rows in the paper era"
            continue
        applied[key] = threshold

    tuned = frame.copy()
    for key, threshold in applied.items():
        feature, direction = PAPER_MAP[key]
        values = tuned[feature]
        tuned = tuned[values.notna() & (values >= threshold if direction == "min" else values <= threshold)]

    actual = metrics(frame)
    tuned_metrics = metrics(tuned)
    removed = frame[~frame.index.isin(tuned.index)]

    pnl_delta = tuned_metrics["total_pnl_sol"] - actual["total_pnl_sol"]
    wr_delta = tuned_metrics["win_rate"] - actual["win_rate"]
    retention = tuned_metrics["trades"] / actual["trades"] * 100.0 if actual["trades"] else 0.0
    avg_delta = tuned_metrics["avg_pnl_sol"] - actual["avg_pnl_sol"]

    checks = {
        "total PnL >= baseline": tuned_metrics["total_pnl_sol"] >= actual["total_pnl_sol"],
        "win rate improved >= 2pp": wr_delta >= WR_IMPROVE_PP,
        "retained >= 40% of trades": retention >= RETENTION_MIN,
        "avg PnL/trade >= baseline": tuned_metrics["avg_pnl_sol"] >= actual["avg_pnl_sol"],
    }
    passed = all(checks.values())

    lines = [
        "# Final Validation — Iteration 3 Gates vs Paper Trading (2026-08-05..2026-08-18)",
        "",
        "> Paper trading data (CLOSED positions in `data/trades.db`, strategy B) was held out through all "
        "three tuning iterations and is touched only here.",
        "",
        f"Tuned gates: `{json.dumps(gates)}`",
        "",
        "## Gate mapping (replay -> paper)",
        "",
        "| gate | applied? | paper feature |",
        "|---|---|---|",
    ]
    for key, threshold in gates.items():
        if key in applied:
            feature, direction = PAPER_MAP[key]
            lines.append(f"| {key} {('>=' if direction == 'min' else '<=')} {threshold:,.3f} | yes | {feature} |")
        else:
            lines.append(f"| {key} | no — {skipped[key]} | — |")
    lines += [
        "",
        "## Actual paper results (what the loop traded)",
        "",
        f"- Trades: **{actual['trades']}**",
        f"- Win rate: **{actual['win_rate']:.1f}%** ({actual['wins']} wins / {actual['losses']} losses)",
        f"- Total PnL: **{actual['total_pnl_sol']:+.6f} SOL**",
        f"- Avg PnL per trade: {actual['avg_pnl_sol']:+.6f} SOL",
        "",
        "## Tuned gates (applied to paper, no adjustment)",
        "",
        f"- Trades: **{tuned_metrics['trades']}** ({retention:.1f}% retention)",
        f"- Win rate: **{tuned_metrics['win_rate']:.1f}%** ({tuned_metrics['wins']} wins / {tuned_metrics['losses']} losses)",
        f"- Total PnL: **{tuned_metrics['total_pnl_sol']:+.6f} SOL**",
        f"- Avg PnL per trade: {tuned_metrics['avg_pnl_sol']:+.6f} SOL",
        "",
        "## Comparison",
        "",
        "| metric | actual | tuned | delta |",
        "|---|---:|---:|---:|",
        f"| trades | {actual['trades']} | {tuned_metrics['trades']} | {tuned_metrics['trades'] - actual['trades']:+d} ({retention:.0f}% retained) |",
        f"| win rate | {actual['win_rate']:.1f}% | {tuned_metrics['win_rate']:.1f}% | {wr_delta:+.1f}pp |",
        f"| total PnL | {actual['total_pnl_sol']:+.6f} SOL | {tuned_metrics['total_pnl_sol']:+.6f} SOL | {pnl_delta:+.6f} SOL |",
        f"| avg PnL/trade | {actual['avg_pnl_sol']:+.6f} SOL | {tuned_metrics['avg_pnl_sol']:+.6f} SOL | {avg_delta:+.6f} SOL |",
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
        f"## Verdict: **{'PASS — tuned gates improve on actual paper results' if passed else 'FAIL — tuned gates do not beat actual paper results on the holdout'}**",
        "",
        "### Removed cohort (paper trades the tuned gates would have excluded)",
        "",
        f"Removed {len(removed)} trades: WR {metrics(removed)['win_rate']:.1f}%, "
        f"PnL {metrics(removed)['total_pnl_sol']:+.6f} SOL, avg {metrics(removed)['avg_pnl_sol']:+.6f} SOL/trade",
        "",
        "### Caveats",
        "",
        "- Paper trades were taken by the live loop with its own gates and at varying sizes/dates; the tuned "
          "cohort is a post-hoc subset comparison, not a re-execution.",
        "- Gates skipped above were not enforceable on paper-era data (see mapping table).",
        "",
    ]
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
