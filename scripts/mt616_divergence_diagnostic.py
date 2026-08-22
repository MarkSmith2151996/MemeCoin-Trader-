#!/usr/bin/env python3
"""MT-616: backtest-to-live divergence diagnostic.

For every live Strategy B entry (trades.db) whose day has an enriched parquet
available, look up the same mint at the same age in the enriched archive and
compare:

  H1 - gate input data: live Jupiter/DexScreener snapshot values (mcap,
       volume, txns, buy/sell ratio, liquidity, score) vs the backtest's
       cumulative parquet values at the same age.
  H2 - prices: live entry_price_sol vs the parquet OHLCV bar at the same
       timestamp, plus a parquet-bar simulation of the shared exit rules
       (2.5x TP / 0.92 hard stop / trailing / 10-min time stop) and its
       win/lose verdict vs the live outcome.

Deliverables:
  data/matched_entries.csv      one row per live entry, both gate-input sets
  data/divergence_breakdown.csv per-trade agreement flags (entry decision,
                                entry price direction, exit outcome)
  DIVERGENCE_DIAGNOSTIC.md      categorized WR-gap breakdown

Detached read-only diagnostic. No runtime, config, or trading-path changes.
"""

from __future__ import annotations

import bisect
import csv
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parent.parent
TRADES_DB = ROOT / "data" / "trades.db"
ENRICHED_DIR = Path(r"/mnt/d/pumpapi-replay/derived/enriched")
SOL_PRICES_CSV = Path(r"/mnt/d/pumpapi-replay/derived/sol_prices.csv")

MAX_AGE_SECONDS = 22 * 60
MIN_MCAP_USD = 5_100.0
MAX_MCAP_USD = 50_000.0
POOL_MIN_SOL = 5.0
MIN_SCORE = 40.0
MIN_VOLUME_USD = 500.0
MIN_VOLUME_TO_MCAP_RATIO = 0.005
MAX_VOLUME_TO_MCAP_RATIO = 50.0
MIN_FEES_SOL_PER_15K_MCAP = 0.3

TAKE_PROFIT_MULTIPLIER = 2.5
HARD_STOP_MULTIPLIER = 0.92
TRAILING_ARM_MULTIPLIER = 1.02
TRAILING_STOP_MULTIPLIER = 0.98
TIME_STOP_MINUTES = 10
BLOCKED_HOURS = {0, 19, 20, 21}
BLOCKED_WEEKDAYS = {2}

# Diagnostic window. Aug 22 has no enriched parquet yet (ETL finalizes a day
# only after all 24 raw hours complete), so the primary window is Aug 21;
# Aug 20 is included as an auxiliary window for sample size.
PRIMARY_DAY = "2026-08-21"
AUX_DAYS = ["2026-08-20", "2026-08-21"]


def finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def positive_number(value: Any) -> float | None:
    number = finite_number(value)
    return number if number is not None and number > 0 else None


def age_adjusted_min_txns(age_seconds: float) -> int:
    if age_seconds < 60:
        return 3
    if age_seconds < 180:
        return 5
    if age_seconds < 300:
        return 8
    if age_seconds < 600:
        return 12
    return 16


def load_sol_prices() -> dict[str, float]:
    prices: dict[str, float] = {}
    with SOL_PRICES_CSV.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            price = positive_number(row.get("sol_usd"))
            if row.get("date") and price is not None:
                prices[row["date"][:10]] = price
    return prices


def load_live_entries() -> list[dict[str, Any]]:
    con = sqlite3.connect(TRADES_DB)
    con.row_factory = sqlite3.Row
    rows: list[dict[str, Any]] = []
    for day in AUX_DAYS:
        start, end = f"{day}T00:00:00", f"{day}T23:59:59.999999"
        cur = con.execute(
            """
            SELECT p.id AS position_id, p.mint_address, p.opened_at, p.closed_at,
                   p.entry_price_sol, p.close_price_sol, p.realized_pnl_sol,
                   p.adjusted_pnl_sol, p.status,
                   c.scan_time, c.age_minutes, c.mcap_usd, c.volume_usd,
                   c.txns_buys, c.txns_sells, c.buy_sell_ratio, c.liquidity_usd,
                   c.price_usd, c.dev_holdings_pct, c.top10_holder_pct,
                   c.gates_passed, c.gates_failed,
                   t.metadata_json AS sell_metadata
            FROM positions p
            LEFT JOIN candidate_log c ON c.position_id = p.id
            LEFT JOIN trades t ON t.mint_address = p.mint_address
                AND t.side = 'SELL'
                AND t.executed_at = (
                    SELECT MAX(executed_at) FROM trades
                    WHERE mint_address = p.mint_address AND side = 'SELL'
                      AND executed_at >= p.opened_at
                )
            WHERE p.strategy = 'B'
              AND p.opened_at >= ? AND p.opened_at <= ?
            """,
            (start, end),
        )
        for row in cur.fetchall():
            entry: dict[str, Any] = dict(row)
            entry["day"] = day
            try:
                meta = json.loads(entry["sell_metadata"] or "{}")
            except json.JSONDecodeError:
                meta = {}
            entry["close_reason"] = (meta.get("metadata") or {}).get("close_reason")
            rows.append(entry)
    con.close()
    return rows


@dataclass
class MintBars:
    time: list[int]
    open: list[float]
    high: list[float]
    low: list[float]
    close: list[float]
    buy_volume_sol: list[float]
    sell_volume_sol: list[float]
    trade_count: list[int]
    seconds_since_birth: list[float]
    market_cap_usd: list[float]
    max_sol_in_pool: list[float]
    pool: list[str]
    graduated: list[bool]
    creator_holdings_pct: list[float]

    @property
    def by_time(self) -> dict[int, int]:
        return {bar_time: index for index, bar_time in enumerate(self.time)}


def load_mint_bars(con: duckdb.DuckDBPyConnection, path: Path, mints: list[str]) -> dict[str, MintBars]:
    if not mints:
        return {}
    source = str(path).replace("\\", "/").replace("'", "''")
    placeholders = ", ".join("?" for _ in mints)
    frame = con.execute(
        f"""SELECT mint, bar_time, open, high, low, close,
                   buy_volume_sol, sell_volume_sol, trade_count,
                   seconds_since_birth, market_cap_usd, max_sol_in_pool,
                   pool, graduated_this_bar, creator_holdings_pct
            FROM read_parquet('{source}')
            WHERE mint IN ({placeholders})
            ORDER BY mint, bar_time""",
        mints,
    ).fetchdf()
    if frame.empty:
        return {}
    by_mint: dict[str, MintBars] = {}
    for mint, group in frame.groupby("mint", sort=False):
        by_mint[str(mint)] = MintBars(
            time=[int(v) for v in group["bar_time"].tolist()],
            open=[float(v) for v in group["open"].tolist()],
            high=[float(v) for v in group["high"].tolist()],
            low=[float(v) for v in group["low"].tolist()],
            close=[float(v) for v in group["close"].tolist()],
            buy_volume_sol=[float(v) for v in group["buy_volume_sol"].tolist()],
            sell_volume_sol=[float(v) for v in group["sell_volume_sol"].tolist()],
            trade_count=[int(v) for v in group["trade_count"].tolist()],
            seconds_since_birth=[float(v) for v in group["seconds_since_birth"].tolist()],
            market_cap_usd=[float(v) if v is not None else float("nan") for v in group["market_cap_usd"].tolist()],
            max_sol_in_pool=[float(v) if v is not None else float("nan") for v in group["max_sol_in_pool"].tolist()],
            pool=[str(v) if v is not None else "" for v in group["pool"].tolist()],
            graduated=[bool(v) for v in group["graduated_this_bar"].tolist()],
            creator_holdings_pct=[float(v) if v is not None else float("nan") for v in group["creator_holdings_pct"].tolist()],
        )
    return by_mint


def cumulative_stats(bars: MintBars, end_index: int) -> dict[str, float | int]:
    """Cumulative buy/sell volume and trade count through end_index (inclusive)."""
    buy = sum(bars.buy_volume_sol[i] for i in range(end_index + 1))
    sell = sum(bars.sell_volume_sol[i] for i in range(end_index + 1))
    txns = sum(bars.trade_count[i] for i in range(end_index + 1))
    return {"buy": buy, "sell": sell, "txns": txns}


def score_from(
    stats: dict[str, float | int],
    age_seconds: float,
    market_cap_usd: float | None,
    sol_usd: float,
) -> float:
    buys = float(stats["buy"]) * sol_usd
    sells = float(stats["sell"]) * sol_usd
    txns = int(stats["txns"])
    vol = buys + sells
    mcap = market_cap_usd if market_cap_usd is not None and math.isfinite(market_cap_usd) else 0.0
    bs_ratio = buys / max(sells, 1.0)
    vol_ratio = vol / mcap if mcap > 0 else 0.0
    min_txns = max(age_adjusted_min_txns(age_seconds), 1)
    score = 0.0
    score += min(bs_ratio / 2.0, 1.0) * 40.0
    score += min(vol_ratio / 0.05, 1.0) * 30.0
    score += min(txns / (4.0 * min_txns), 1.0) * 15.0
    score += min(vol / (10.0 * max(MIN_VOLUME_USD, 1.0)), 1.0) * 15.0
    return round(score, 1)


def passes_gates(
    stats: dict[str, float | int],
    bar: dict[str, Any],
    sol_usd: float,
) -> tuple[bool, list[str]]:
    """Replay the MT-606 strategy-BT gate semantics at one parquet bar."""
    failed: list[str] = []
    age_seconds = bar["age"]
    mcap = bar["mcap"]
    if age_seconds is None or not 0 <= age_seconds <= MAX_AGE_SECONDS:
        failed.append("age")
    if mcap is None or not MIN_MCAP_USD <= mcap <= MAX_MCAP_USD:
        failed.append("mcap")
    pool_sol = bar["pool"]
    if pool_sol is None or pool_sol < POOL_MIN_SOL:
        failed.append("pool")
    cumulative_volume_usd = (float(stats["buy"]) + float(stats["sell"])) * sol_usd
    if int(stats["txns"]) < age_adjusted_min_txns(age_seconds if age_seconds is not None else 0):
        failed.append("txns")
    if cumulative_volume_usd < MIN_VOLUME_USD:
        failed.append("volume")
    if mcap and mcap > 0:
        ratio = cumulative_volume_usd / mcap
        if not MIN_VOLUME_TO_MCAP_RATIO <= ratio <= MAX_VOLUME_TO_MCAP_RATIO:
            failed.append("vol_mcap")
    if float(stats["sell"]) <= 0 or float(stats["buy"]) / max(float(stats["sell"]), 1.0) < 0.5:
        failed.append("buy_sell")
    score = score_from(stats, age_seconds if age_seconds is not None else 0, mcap, sol_usd)
    if score < MIN_SCORE:
        failed.append("score")
    creator = bar["creator"]
    if creator is not None and creator > 0:
        failed.append("creator")
    bar_time_ms = bar["time"]
    current_time = datetime.fromtimestamp(bar_time_ms / 1000.0, UTC)
    if current_time.hour in BLOCKED_HOURS or current_time.weekday() in BLOCKED_WEEKDAYS:
        failed.append("blocked_time")
    return (not failed), failed


def bar_at(bars: MintBars, timestamp_ms: int) -> tuple[int, dict[str, Any]] | None:
    """Nearest bar index + fields at/around timestamp_ms (prefer the bar whose
    window contains the timestamp; bar_time is the bar's end)."""
    index = bisect.bisect_left(bars.time, timestamp_ms)
    if index >= len(bars.time):
        return None
    if index == 0:
        return index, bar_fields(bars, index)
    prev = index - 1
    if timestamp_ms - bars.time[prev] <= bars.time[index] - timestamp_ms:
        return prev, bar_fields(bars, prev)
    return index, bar_fields(bars, index)


def bar_fields(bars: MintBars, index: int) -> dict[str, Any]:
    return {
        "time": bars.time[index],
        "open": bars.open[index],
        "close": bars.close[index],
        "age": finite_number(bars.seconds_since_birth[index]),
        "mcap": finite_number(bars.market_cap_usd[index]),
        "pool": positive_number(bars.max_sol_in_pool[index]),
        "pool_label": bars.pool[index],
        "graduated": bars.graduated[index],
        "creator": finite_number(bars.creator_holdings_pct[index]),
    }


def simulate_exit(
    bars: MintBars,
    entry_index: int,
    entry_price: float,
    scan_time_ms: int,
) -> dict[str, Any]:
    """Shared MT-607 exit rules on parquet bars (30s sampling cadence like the
    backtest) starting from entry_index. Reference price = entry_price."""
    window_end = bars.time[entry_index] + TIME_STOP_MINUTES * 60 * 1000
    highest = entry_price
    for index in range(entry_index, len(bars.time)):
        bar_time = bars.time[index]
        if bar_time > window_end:
            break
        if (bar_time - bars.time[entry_index]) % 30_000 >= 5_000:
            continue
        close = bars.close[index]
        highest = max(highest, close)
        if close >= entry_price * TAKE_PROFIT_MULTIPLIER:
            return {"reason": "take_profit", "price": entry_price * TAKE_PROFIT_MULTIPLIER,
                    "time": bar_time, "index": index}
        if close <= entry_price * HARD_STOP_MULTIPLIER:
            return {"reason": "hard_stop", "price": entry_price * HARD_STOP_MULTIPLIER,
                    "time": bar_time, "index": index}
        if highest >= entry_price * TRAILING_ARM_MULTIPLIER and close <= highest * TRAILING_STOP_MULTIPLIER:
            return {"reason": "trailing_stop", "price": highest * TRAILING_STOP_MULTIPLIER,
                    "time": bar_time, "index": index}
        if bar_time - bars.time[entry_index] >= TIME_STOP_MINUTES * 60 * 1000:
            return {"reason": "time_stop", "price": close, "time": bar_time, "index": index}
    return {"reason": "no_exit_within_window", "price": None, "time": None, "index": None}


def parse_time_ms(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def main() -> None:
    sol_prices = load_sol_prices()
    entries = load_live_entries()
    print(f"live entries loaded: {len(entries)}")
    by_day: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_day.setdefault(entry["day"], []).append(entry)

    con = duckdb.connect()
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 4")

    all_matched: list[dict[str, Any]] = []
    day_summaries: list[dict[str, Any]] = []

    for day in AUX_DAYS:
        day_entries = by_day.get(day, [])
        if not day_entries:
            continue
        path = ENRICHED_DIR / f"{day}.parquet"
        if not path.exists():
            print(f"[{day}] enriched parquet missing; skipping {len(day_entries)} entries")
            continue
        sol_usd = sol_prices.get(day)
        if sol_usd is None:
            print(f"[{day}] no SOL price; skipping")
            continue
        mints = sorted({e["mint_address"] for e in day_entries})
        print(f"[{day}] {len(day_entries)} entries, {len(mints)} unique mints")
        bars_by_mint = load_mint_bars(con, path, mints)
        print(f"[{day}] bars loaded for {len(bars_by_mint)} mints")

        day_matched: list[dict[str, Any]] = []
        unmatched = 0
        for entry in day_entries:
            mint = entry["mint_address"]
            bars = bars_by_mint.get(mint)
            if bars is None or not bars.time:
                unmatched += 1
                entry["parquet_found"] = False
                day_matched.append(build_row(entry, None, None, None, sol_usd))
                continue
            entry_ms = parse_time_ms(entry["opened_at"])
            found = bar_at(bars, entry_ms)
            if found is None:
                unmatched += 1
                entry["parquet_found"] = False
                day_matched.append(build_row(entry, bars, None, None, sol_usd))
                continue
            gate_index, gate_bar = found
            stats = cumulative_stats(bars, gate_index)
            entry["parquet_found"] = True
            row = build_row(entry, bars, (gate_index, gate_bar, stats), None, sol_usd)
            day_matched.append(row)

        day_summaries.append(summarize_day(day, day_matched))
        all_matched.extend(day_matched)

    write_outputs(all_matched, day_summaries)


def build_row(
    entry: dict[str, Any],
    bars: MintBars | None,
    gate: tuple[int, dict[str, Any], dict[str, float | int]] | None,
    _unused: Any,
    sol_usd: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "day": entry["day"],
        "position_id": entry["position_id"],
        "mint": entry["mint_address"],
        "opened_at": entry["opened_at"],
        "closed_at": entry["closed_at"],
        "live_entry_price_sol": entry["entry_price_sol"],
        "live_close_price_sol": entry["close_price_sol"],
        "live_pnl_sol": entry["realized_pnl_sol"],
        "live_close_reason": entry["close_reason"] or "",
        "live_win": 1 if (entry["realized_pnl_sol"] or 0) > 0 else 0,
        # Live gate inputs (what Jupiter/DexScreener reported at scan time)
        "live_age_minutes": entry["age_minutes"],
        "live_mcap_usd": entry["mcap_usd"],
        "live_volume_usd": entry["volume_usd"],
        "live_txns_buys": entry["txns_buys"],
        "live_txns_sells": entry["txns_sells"],
        "live_txns_total": (entry["txns_buys"] or 0) + (entry["txns_sells"] or 0),
        "live_buy_sell_ratio": entry["buy_sell_ratio"],
        "live_liquidity_usd": entry["liquidity_usd"],
        "live_dev_holdings_pct": entry["dev_holdings_pct"],
        "live_top10_holder_pct": entry["top10_holder_pct"],
        "parquet_found": entry["parquet_found"],
    }
    if not entry["parquet_found"]:
        for field in (
            "bt_gate_bar_time", "bt_age_seconds", "bt_mcap_usd", "bt_pool_sol",
            "bt_cum_buy_sol", "bt_cum_sell_sol", "bt_cum_txns", "bt_volume_usd",
            "bt_bs_ratio", "bt_score", "bt_gate_pass", "bt_gate_failed",
            "entry_price_gap_pct_close", "entry_price_gap_pct_next_open",
            "sim_exit_reason", "sim_exit_price_sol", "sim_win", "exit_outcome_agree",
            "entry_decision_agree", "entry_price_direction",
        ):
            row[field] = ""
        return row

    gate_index, gate_bar, stats = gate  # type: ignore[misc]
    bt_pass, bt_failed = passes_gates(stats, gate_bar, sol_usd)
    bt_score = score_from(stats, gate_bar["age"] or 0, gate_bar["mcap"], sol_usd)
    cum_vol_usd = (float(stats["buy"]) + float(stats["sell"])) * sol_usd
    bt_bs = float(stats["buy"]) / max(float(stats["sell"]), 1.0)

    row.update({
        "bt_gate_bar_time": gate_bar["time"],
        "bt_age_seconds": gate_bar["age"],
        "bt_mcap_usd": gate_bar["mcap"],
        "bt_pool_sol": gate_bar["pool"],
        "bt_cum_buy_sol": round(float(stats["buy"]), 2),
        "bt_cum_sell_sol": round(float(stats["sell"]), 2),
        "bt_cum_txns": int(stats["txns"]),
        "bt_volume_usd": round(cum_vol_usd, 0),
        "bt_bs_ratio": round(bt_bs, 3),
        "bt_score": bt_score,
        "bt_gate_pass": 1 if bt_pass else 0,
        "bt_gate_failed": ";".join(bt_failed),
    })

    # H2 entry price: live entry vs parquet bar close at same timestamp, and
    # vs the next bar's open (the backtest's actual entry reference).
    entry_ms = parse_time_ms(entry["opened_at"])
    gap_close = None
    if gate_bar["close"]:
        gap_close = (float(entry["entry_price_sol"]) / gate_bar["close"] - 1.0) * 100.0
    next_open = None
    if gate_index + 1 < len(bars.time):  # type: ignore[union-attr]
        next_open = bars.open[gate_index + 1]  # type: ignore[union-attr]
    gap_next_open = (float(entry["entry_price_sol"]) / next_open - 1.0) * 100.0 if next_open else None
    row["entry_price_gap_pct_close"] = round(gap_close, 2) if gap_close is not None else ""
    row["entry_price_gap_pct_next_open"] = round(gap_next_open, 2) if gap_next_open is not None else ""

    # Simulate the shared exit rules on parquet bars from the entry bar.
    sim = simulate_exit(bars, gate_index, float(entry["entry_price_sol"]), entry_ms)  # type: ignore[union-attr]
    sim_win = 1 if sim["price"] is not None and sim["price"] > float(entry["entry_price_sol"]) else 0
    row["sim_exit_reason"] = sim["reason"]
    row["sim_exit_price_sol"] = sim["price"]
    row["sim_win"] = sim_win

    # Agreement flags.
    live_win = row["live_win"]
    row["entry_decision_agree"] = 1 if bt_pass else 0
    if gap_close is not None:
        row["entry_price_direction"] = "live_higher" if gap_close > 0 else "live_lower"
    else:
        row["entry_price_direction"] = ""
    if sim["price"] is not None:
        row["exit_outcome_agree"] = 1 if sim_win == live_win else 0
    else:
        row["exit_outcome_agree"] = ""
    return row


def summarize_day(day: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [r for r in rows if r["parquet_found"]]
    live_wr = 100.0 * sum(r["live_win"] for r in matched) / len(matched) if matched else 0.0
    bt_gate_pass_rate = 100.0 * sum(r.get("bt_gate_pass", 0) == 1 for r in matched) / len(matched) if matched else 0.0
    sim_wr = 100.0 * sum(r.get("sim_win", "") == 1 for r in matched) / len(matched) if matched else 0.0
    agree_exit = [r for r in matched if r.get("exit_outcome_agree") != ""]
    exit_agree_pct = 100.0 * sum(r["exit_outcome_agree"] for r in agree_exit) / len(agree_exit) if agree_exit else 0.0
    return {
        "day": day,
        "total_entries": len(rows),
        "matched": len(matched),
        "live_wr_pct": round(live_wr, 2),
        "bt_gate_pass_rate_pct": round(bt_gate_pass_rate, 2),
        "sim_wr_pct": round(sim_wr, 2),
        "exit_agree_pct": round(exit_agree_pct, 2),
    }


def write_outputs(rows: list[dict[str, Any]], day_summaries: list[dict[str, Any]]) -> None:
    data_dir = ROOT / "data"
    data_dir.mkdir(exist_ok=True)
    matched_csv = data_dir / "matched_entries.csv"
    breakdown_csv = data_dir / "divergence_breakdown.csv"

    fieldnames = [
        "day", "position_id", "mint", "opened_at", "closed_at",
        "live_entry_price_sol", "live_close_price_sol", "live_pnl_sol",
        "live_close_reason", "live_win",
        "live_age_minutes", "live_mcap_usd", "live_volume_usd",
        "live_txns_buys", "live_txns_sells", "live_txns_total",
        "live_buy_sell_ratio", "live_liquidity_usd",
        "live_dev_holdings_pct", "live_top10_holder_pct",
        "bt_gate_bar_time", "bt_age_seconds", "bt_mcap_usd", "bt_pool_sol",
        "bt_cum_buy_sol", "bt_cum_sell_sol", "bt_cum_txns", "bt_volume_usd",
        "bt_bs_ratio", "bt_score", "bt_gate_pass", "bt_gate_failed",
        "entry_price_gap_pct_close", "entry_price_gap_pct_next_open",
        "sim_exit_reason", "sim_exit_price_sol", "sim_win",
        "entry_decision_agree", "entry_price_direction", "exit_outcome_agree",
    ]
    with matched_csv.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    breakdown_fields = [
        "day", "position_id", "mint", "opened_at",
        "live_entry_price_sol", "live_close_price_sol", "live_pnl_sol",
        "live_close_reason", "live_win",
        "bt_gate_pass", "entry_decision_agree",
        "entry_price_gap_pct_close", "entry_price_direction",
        "sim_exit_reason", "sim_win", "exit_outcome_agree",
        "bt_gate_failed",
    ]
    with breakdown_csv.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=breakdown_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print("\n== per-day summary ==")
    for summary in day_summaries:
        print(summary)
    print(f"\nwrote {len(rows)} rows -> {matched_csv}")
    print(f"wrote {len(rows)} rows -> {breakdown_csv}")


if __name__ == "__main__":
    main()
