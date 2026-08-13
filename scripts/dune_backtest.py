#!/usr/bin/env python3
"""Read Dune graduation exports and replay observable Strategy B behavior.

This is intentionally detached from the trading runtime. It neither opens
positions nor writes to the runtime SQLite database.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = ROOT / "data" / "dune"
DEFAULT_OUTPUT_DIR = ROOT / "analysis" / "dune_backtest_output"
DEFAULT_PAPER_DB = ROOT / "data" / "trades.db"

# Strategy B values as of MT-520. These are duplicated deliberately so this
# analysis script never imports or mutates the running Strategy B process.
MAX_AGE_MINUTES = 20.0
MIN_MCAP_USD = 1_000.0
MAX_MCAP_USD = 50_000.0
MIN_VOLUME_USD = 500.0
MIN_BUY_SELL_RATIO = 0.4
TRAILING_STOP_PCT = 4.0
TRAILING_ARM_PCT = 2.0
TAKE_PROFIT_PCT = 80.0
HARD_STOP_PCT = 10.0
EARLY_EXIT_SECONDS = 90.0
EARLY_EXIT_GREEN_PCT = 0.01
PAPER_SIZE_SOL = 0.05


@dataclass(frozen=True)
class Graduation:
    mint_address: str
    graduation_timestamp: datetime | None
    age_minutes: float | None
    market_cap_usd: float | None
    volume_usd: float | None
    buys: int | None
    sells: int | None
    liquidity_proxy_usd: float | None


@dataclass(frozen=True)
class Swap:
    mint_address: str
    timestamp: datetime
    price_sol: float


def _value(row: dict[str, str], *names: str) -> str | None:
    normalized = {key.strip().lower(): value for key, value in row.items() if key}
    for name in names:
        value = normalized.get(name.lower())
        if value is not None and value.strip():
            return value.strip()
    return None


def _number(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value.replace(",", "").replace("$", ""))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _integer(value: str | None) -> int | None:
    parsed = _number(value)
    return int(parsed) if parsed is not None else None


def _timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def _age_adjusted_min_txns(age_minutes: float) -> int:
    if age_minutes < 1:
        return 3
    if age_minutes < 3:
        return 5
    if age_minutes < 5:
        return 8
    if age_minutes < 10:
        return 12
    return 16


def read_graduations(path: Path) -> list[Graduation]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path} has no CSV header")
        results = []
        for row in reader:
            mint = _value(row, "mint_address", "mint")
            if not mint:
                continue
            market_cap = _number(_value(
                row, "market_cap_usd_at_graduation", "market_cap_usd", "mcap_usd",
            ))
            min_price_usd = _number(_value(row, "min_price_usd"))
            results.append(Graduation(
                mint_address=mint,
                graduation_timestamp=_timestamp(_value(row, "graduation_timestamp", "first_trade")),
                age_minutes=_number(_value(row, "age_minutes_at_graduation", "age_minutes")),
                # Query A V1 exposes token price but not token supply. Pump.fun
                # conventionally uses 1B supply, so retain this as an estimate.
                market_cap_usd=market_cap if market_cap is not None else (
                    min_price_usd * 1_000_000_000 if min_price_usd is not None else None
                ),
                volume_usd=_number(_value(
                    row, "volume_usd_first_30m", "volume_usd", "total_volume_usd",
                )),
                buys=_integer(_value(row, "buy_count_first_30m", "buys", "buy_count")),
                sells=_integer(_value(row, "sell_count_first_30m", "sells", "sell_count")),
                liquidity_proxy_usd=_number(_value(
                    row, "liquidity_added_usd_proxy", "liquidity_added_usd", "liquidity_usd",
                )),
            ))
    return results


def read_swaps(path: Path) -> dict[str, list[Swap]]:
    grouped: dict[str, list[Swap]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path} has no CSV header")
        for row in reader:
            mint = _value(row, "mint_address", "mint")
            timestamp = _timestamp(_value(row, "timestamp", "block_time"))
            price = _number(_value(row, "price_sol", "price_in_sol"))
            if mint and timestamp and price is not None and price > 0:
                grouped[mint].append(Swap(mint, timestamp, price))
    for swaps in grouped.values():
        swaps.sort(key=lambda swap: swap.timestamp)
    return grouped


def gate_result(graduation: Graduation) -> tuple[bool, list[str], dict[str, object]]:
    age = graduation.age_minutes
    age_source = "exported_launch_to_graduation"
    if age is None:
        # The Dune withdrawal is the backtest signal; without a create time the
        # observable signal age is zero, not a claim about original launch age.
        age = 0.0
        age_source = "assumed_zero_at_graduation"
    mcap = graduation.market_cap_usd
    volume = graduation.volume_usd
    buys = graduation.buys
    sells = graduation.sells
    txns = buys + sells if buys is not None and sells is not None else None
    buy_sell_ratio = buys / max(sells, 1) if buys is not None and sells is not None else None
    gates = {
        "age_pass": age <= MAX_AGE_MINUTES,
        "mcap_pass": mcap is not None and MIN_MCAP_USD <= mcap <= MAX_MCAP_USD,
        "volume_pass": volume is not None and volume >= MIN_VOLUME_USD,
        "txn_pass": txns is not None and txns >= _age_adjusted_min_txns(age),
        "buy_sell_pass": buy_sell_ratio is not None and buy_sell_ratio >= MIN_BUY_SELL_RATIO,
        "age_source": age_source,
        "age_minutes": age,
        "txns": txns,
        "buy_sell_ratio": buy_sell_ratio,
        "unobservable_live_gates": "rugcheck,holder,creator,mentions,time_gate,repeat_loser",
    }
    failed = [name for name in ("age_pass", "mcap_pass", "volume_pass", "txn_pass", "buy_sell_pass")
              if not gates[name]]
    return not failed, failed, gates


def replay_exit(mint: str, swaps: Iterable[Swap]) -> dict[str, object] | None:
    prices = list(swaps)
    if not prices:
        return None
    entry = prices[0]
    peak = entry.price_sol
    close = prices[-1]
    reason = "open_at_end"
    for swap in prices:
        peak = max(peak, swap.price_sol)
        elapsed = (swap.timestamp - entry.timestamp).total_seconds()
        if swap.price_sol >= entry.price_sol * (1 + TAKE_PROFIT_PCT / 100):
            close = Swap(mint, swap.timestamp, entry.price_sol * (1 + TAKE_PROFIT_PCT / 100))
            reason = "take_profit"
            break
        if swap.price_sol <= entry.price_sol * (1 - HARD_STOP_PCT / 100):
            close = Swap(mint, swap.timestamp, entry.price_sol * (1 - HARD_STOP_PCT / 100))
            reason = "hard_stop"
            break
        if (peak > entry.price_sol * (1 + TRAILING_ARM_PCT / 100)
                and (peak - swap.price_sol) / peak >= TRAILING_STOP_PCT / 100):
            close = swap
            reason = "trailing_stop"
            break
        if elapsed >= EARLY_EXIT_SECONDS and peak <= entry.price_sol * (1 + EARLY_EXIT_GREEN_PCT / 100):
            close = swap
            reason = "early_exit_no_green"
            break
    pnl_pct = (close.price_sol / entry.price_sol - 1) * 100
    return {
        "mint_address": mint,
        "entry_timestamp": entry.timestamp.isoformat(),
        "entry_price_sol": entry.price_sol,
        "exit_timestamp": close.timestamp.isoformat(),
        "exit_price_sol": close.price_sol,
        "peak_price_sol": peak,
        "exit_reason": reason,
        "pnl_pct": pnl_pct,
        "pnl_sol_at_0_05_size": PAPER_SIZE_SOL * pnl_pct / 100,
        "closed_within_two_hours": reason != "open_at_end",
    }


def paper_comparison(db_path: Path) -> dict[str, object]:
    if not db_path.exists():
        return {"available": False, "reason": f"paper database not found: {db_path}"}
    try:
        with sqlite3.connect(db_path) as db:
            row = db.execute(
                """SELECT COUNT(*), COALESCE(SUM(realized_pnl_sol), 0),
                          COALESCE(SUM(CASE WHEN realized_pnl_sol > 0 THEN 1 ELSE 0 END), 0)
                   FROM positions
                   WHERE strategy = 'B' AND status = 'CLOSED'""",
            ).fetchone()
    except sqlite3.Error as exc:
        return {"available": False, "reason": f"could not read paper database: {exc}"}
    trades, pnl_sol, wins = (int(row[0]), float(row[1]), int(row[2]))
    return {
        "available": True,
        "closed_trades": trades,
        "total_pnl_sol": pnl_sol,
        "win_rate": wins / trades if trades else None,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    columns = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def run(graduations_path: Path, swaps_path: Path, output_dir: Path, paper_db: Path) -> dict[str, object]:
    graduations = read_graduations(graduations_path)
    swaps_by_mint = read_swaps(swaps_path)
    rows: list[dict[str, object]] = []
    exit_counts: Counter[str] = Counter()
    realized_pnl_sol = 0.0
    closed_pnls: list[float] = []

    for graduation in graduations:
        passed, failed, gates = gate_result(graduation)
        row: dict[str, object] = {
            "mint_address": graduation.mint_address,
            "graduation_timestamp": (
                graduation.graduation_timestamp.isoformat() if graduation.graduation_timestamp else ""
            ),
            "gate_passed": passed,
            "failed_gates": ",".join(failed),
            "market_cap_usd": graduation.market_cap_usd,
            "volume_usd_first_30m": graduation.volume_usd,
            "buys_first_30m": graduation.buys,
            "sells_first_30m": graduation.sells,
            "liquidity_added_usd_proxy": graduation.liquidity_proxy_usd,
            **gates,
        }
        if passed:
            replay = replay_exit(graduation.mint_address, swaps_by_mint.get(graduation.mint_address, []))
            if replay is None:
                row["exit_reason"] = "no_valid_price_path"
            else:
                row.update(replay)
                exit_counts[str(replay["exit_reason"])] += 1
                if replay["closed_within_two_hours"]:
                    pnl = float(replay["pnl_sol_at_0_05_size"])
                    realized_pnl_sol += pnl
                    closed_pnls.append(pnl)
        rows.append(row)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "per_trade_results.csv", rows)
    passed_rows = [row for row in rows if row["gate_passed"]]
    comparable = len(closed_pnls)
    summary = {
        "input": {
            "graduations_csv": str(graduations_path),
            "swaps_csv": str(swaps_path),
            "graduations_loaded": len(graduations),
            "mints_with_valid_swaps": len(swaps_by_mint),
        },
        "strategy_b_parameters": {
            "max_age_minutes": MAX_AGE_MINUTES,
            "min_mcap_usd": MIN_MCAP_USD,
            "max_mcap_usd": MAX_MCAP_USD,
            "min_volume_usd": MIN_VOLUME_USD,
            "min_buy_sell_ratio": MIN_BUY_SELL_RATIO,
            "trailing_stop_pct": TRAILING_STOP_PCT,
            "trailing_arm_pct": TRAILING_ARM_PCT,
            "take_profit_pct": TAKE_PROFIT_PCT,
            "hard_stop_pct": HARD_STOP_PCT,
            "early_exit_seconds": EARLY_EXIT_SECONDS,
            "early_exit_green_pct": EARLY_EXIT_GREEN_PCT,
            "paper_size_sol": PAPER_SIZE_SOL,
        },
        "dune_backtest": {
            "gate_passed": len(passed_rows),
            "gate_pass_rate": len(passed_rows) / len(rows) if rows else None,
            "closed_within_two_hours": comparable,
            "total_realized_pnl_sol_at_0_05_size": realized_pnl_sol,
            "win_rate_closed_only": (
                sum(pnl > 0 for pnl in closed_pnls) / comparable if comparable else None
            ),
            "exit_reasons": dict(exit_counts),
            "unrealized_open_at_two_hours": exit_counts["open_at_end"],
        },
        "paper_trading_comparison": paper_comparison(paper_db),
        "limitations": [
            "Entries are the first recorded post-graduation wSOL swap, not a simulated live quote.",
            "The Dune export cannot reproduce Strategy B RugCheck, holder, creator, Grok, UTC-hour, or repeat-loser gates.",
            "Market cap is the Query A 1B-supply estimate; liquidity_added_usd_proxy is first-trade notional, not a pool reserve snapshot.",
            "Only exits reached within the two-hour export are included in realized backtest PnL; open_at_end paths are reported separately.",
        ],
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--graduations", type=Path, help="Query A CSV (default: input-dir/graduated_tokens.csv)")
    parser.add_argument("--swaps", type=Path, help="Query B CSV (default: input-dir/token_swaps.csv)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--paper-db", type=Path, default=DEFAULT_PAPER_DB)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    graduations = args.graduations or args.input_dir / "graduated_tokens.csv"
    swaps = args.swaps or args.input_dir / "token_swaps.csv"
    missing = [str(path) for path in (graduations, swaps) if not path.is_file()]
    if missing:
        raise SystemExit("Missing Dune CSV export(s): " + ", ".join(missing))
    summary = run(graduations, swaps, args.output_dir, args.paper_db)
    backtest = summary["dune_backtest"]
    print(f"Loaded {summary['input']['graduations_loaded']} graduated tokens.")
    print(f"Passed observable Strategy B gates: {backtest['gate_passed']}.")
    print(
        "Closed within 2h: "
        f"{backtest['closed_within_two_hours']}; realized PnL: "
        f"{backtest['total_realized_pnl_sol_at_0_05_size']:+.6f} SOL."
    )
    print(f"Wrote {args.output_dir / 'per_trade_results.csv'} and summary.json")


if __name__ == "__main__":
    main()
