"""MT-591 step 1: extract entry-time features for closed paper positions.

For every CLOSED position in a requested window, join the entry-time
candidate_log row and produce one feature row with the signal available at
entry (mcap, volume, txns, buy/sell ratio, liquidity, age, holder/dev
concentration, price changes, graduation proxy) plus the trade outcome
(realized PnL, win label). Strictly uses entry-time fields only — nothing
observed after entry. Offline, read-only.

Usage:
    python3 scripts/walk_forward_extract.py
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "trades.db"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "walk_forward"

MIN_VOLUME_USD = 500.0
MIN_TXNS = 3


def age_adjusted_min_txns(age_minutes: float) -> int:
    if age_minutes < 1.0:
        return 3
    if age_minutes < 3.0:
        return 5
    if age_minutes < 5.0:
        return 8
    if age_minutes < 10.0:
        return 12
    return 16


def strength_score_proxy(bs_ratio: float, vol_usd: float, mcap_usd: float, txns: int, age_minutes: float) -> float:
    """Replica of run_strategy_b._candidate_strength_score over stored fields.

    The live composite uses Jupiter h1 pairs; here the same component math is
    applied to the persisted candidate_log snapshot fields (an approximation —
    the stored volume bucket may differ from h1).
    """
    vol_ratio = vol_usd / mcap_usd if mcap_usd and mcap_usd > 0 else 0.0
    min_txns = max(age_adjusted_min_txns(age_minutes), 1)
    score = 0.0
    score += min(bs_ratio / 2.0, 1.0) * 40.0
    score += min(vol_ratio / 0.05, 1.0) * 30.0
    score += min(txns / (4.0 * min_txns), 1.0) * 15.0
    score += min(vol_usd / (10.0 * MIN_VOLUME_USD), 1.0) * 15.0
    return round(score, 1)


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def extract_window(db: sqlite3.Connection, start: date, end: date) -> list[dict]:
    query = """
        SELECT p.id AS position_id, p.mint_address, p.amount_sol, p.entry_price_sol,
               p.opened_at, p.closed_at, p.realized_pnl_sol, p.adjusted_pnl_sol,
               p.close_price_sol, p.peak_price_sol, p.strategy,
               c.ticker, c.age_minutes, c.mcap_usd, c.volume_usd, c.txns_buys, c.txns_sells,
               c.buy_sell_ratio, c.liquidity_usd, c.fdv, c.price_change_5m, c.price_change_1h,
               c.dev_holdings_pct, c.top10_holder_pct, c.rugcheck_result, c.profile
        FROM positions p
        LEFT JOIN candidate_log c ON c.position_id = p.id
        WHERE p.status = 'CLOSED'
          AND date(p.opened_at) BETWEEN ? AND ?
          AND p.strategy = 'B'
        ORDER BY p.opened_at
    """
    rows = db.execute(query, (start.isoformat(), end.isoformat())).fetchall()
    columns = [
        "position_id", "mint_address", "amount_sol", "entry_price_sol", "opened_at", "closed_at",
        "realized_pnl_sol", "adjusted_pnl_sol", "close_price_sol", "peak_price_sol", "strategy",
        "ticker", "age_minutes", "mcap_usd", "volume_usd", "txns_buys", "txns_sells",
        "buy_sell_ratio", "liquidity_usd", "fdv", "price_change_5m", "price_change_1h",
        "dev_holdings_pct", "top10_holder_pct", "rugcheck_result", "profile",
    ]
    close_reason_by_mint: dict[str, str] = {}
    for row in db.execute(
        """SELECT mint_address, json_extract(metadata_json, '$.metadata.close_reason')
           FROM trades
           WHERE side = 'SELL' AND status = 'simulated'
             AND json_extract(metadata_json, '$.metadata.close_reason') IS NOT NULL
             AND executed_at >= ?
           ORDER BY executed_at ASC""",
        (start.isoformat(),),
    ).fetchall():
        close_reason_by_mint.setdefault(str(row[0]), str(row[1]))

    records: list[dict] = []
    for row in rows:
        record = dict(zip(columns, row, strict=True))
        mint = str(record["mint_address"])
        age_min = finite(record["age_minutes"])
        mcap = finite(record["mcap_usd"])
        vol = finite(record["volume_usd"])
        buys = finite(record["txns_buys"])
        sells = finite(record["txns_sells"])
        bs = finite(record["buy_sell_ratio"])
        txns = (buys or 0.0) + (sells or 0.0) if buys is not None and sells is not None else None
        if bs is None and buys is not None and sells is not None:
            bs = buys / sells if sells and sells > 0 else (float("inf") if buys and buys > 0 else None)

        record["txns_total"] = txns
        record["vol_mcap_ratio"] = vol / mcap if vol is not None and mcap not in (None, 0) else None
        if age_min is None:
            record["score_proxy"] = None
        elif bs is None or vol is None or mcap is None or txns is None:
            record["score_proxy"] = None
        else:
            record["score_proxy"] = strength_score_proxy(bs, vol, mcap, txns, age_min)
        record["mint_is_pump"] = 1 if str(record["mint_address"]).endswith("pump") else 0
        record["win"] = 1 if finite(record["realized_pnl_sol"]) and finite(record["realized_pnl_sol"]) > 0 else 0
        record["close_reason"] = close_reason_by_mint.get(mint, "")
        records.append(record)
    return records


def coverage_report(records: list[dict]) -> str:
    total = len(records)
    wins = sum(r["win"] for r in records)
    pnl = sum(finite(r["realized_pnl_sol"]) or 0.0 for r in records)
    lines = [
        f"positions={total} wins={wins} win_rate={wins / total * 100:.1f}% total_pnl={pnl:+.6f} SOL",
    ]
    for field in ("mcap_usd", "volume_usd", "buy_sell_ratio", "txns_total", "age_minutes",
                  "liquidity_usd", "top10_holder_pct", "dev_holdings_pct", "vol_mcap_ratio", "score_proxy"):
        missing = sum(1 for r in records if r[field] is None)
        lines.append(f"  {field}: {total - missing}/{total} populated ({missing} missing)")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--start", help="Extract a single window: start YYYY-MM-DD.")
    parser.add_argument("--end", help="Extract a single window: end YYYY-MM-DD (inclusive).")
    parser.add_argument("--out-csv", type=Path, help="Output CSV when --start/--end are given.")
    args = parser.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(args.db)
    try:
        if args.start and args.end:
            window = extract_window(connection, date.fromisoformat(args.start), date.fromisoformat(args.end))
        else:
            week1 = extract_window(connection, date(2026, 8, 5), date(2026, 8, 11))
            week2 = extract_window(connection, date(2026, 8, 12), date(2026, 8, 18))
    finally:
        connection.close()

    fields = [
        "position_id", "mint_address", "ticker", "strategy", "amount_sol", "entry_price_sol",
        "opened_at", "closed_at", "close_reason", "age_minutes", "mcap_usd", "volume_usd",
        "txns_buys", "txns_sells", "txns_total", "buy_sell_ratio", "liquidity_usd", "fdv",
        "price_change_5m", "price_change_1h", "dev_holdings_pct", "top10_holder_pct",
        "rugcheck_result", "profile", "vol_mcap_ratio", "score_proxy", "mint_is_pump",
        "realized_pnl_sol", "adjusted_pnl_sol", "win",
    ]
    if args.start and args.end:
        output = args.out_csv or (out_dir / f"{args.start}_{args.end}_features.csv")
        with output.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(window)
        print(f"WINDOW ({args.start}..{args.end}):")
        print(coverage_report(window))
        print(f"Wrote {output} ({len(window)} rows)")
        return

    with (out_dir / "week1_features.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(week1)
    with (out_dir / "week2_features.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(week2)

    print("WEEK 1 (train, 2026-08-05..2026-08-11):")
    print(coverage_report(week1))
    print("WEEK 2 (blind test, 2026-08-12..2026-08-18):")
    print(coverage_report(week2))
    print(f"Wrote {out_dir / 'week1_features.csv'} ({len(week1)} rows) and "
          f"{out_dir / 'week2_features.csv'} ({len(week2)} rows)")

    frame = pd.DataFrame(week1)
    frame.to_csv(out_dir / "week1_features_full.csv", index=False)


if __name__ == "__main__":
    main()
