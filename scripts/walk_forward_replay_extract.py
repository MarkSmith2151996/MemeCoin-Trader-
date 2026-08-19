"""MT-592: extract entry-feature trades from the 4-month replay archive.

Faithful port of ``D:\\pumpapi-replay\\replay_stratb.py`` (Strategy B gate
rules, candidate selection, entry/exit simulation) with two additions:

1. **Entry-time feature snapshot** — for every resolved trade, records the
   decision features available at the gate-passing scan (mcap, age, cumulative
   volumes, buy/sell ratio, trade count, holder/dev concentration, pool SOL,
   two score variants) so downstream tuners learn only entry-time signals.
2. **MT-569 friction model** — PnL is computed with 3% slippage plus
   pool-relative market impact on entry and 3% slippage on exit (identical to
   ``replay.py`` trade_row), so tuned-vs-baseline PnL is comparable to the
   MT-569 baseline.

The bar fetching is batched per day (one query for all candidate mints instead
of one query per trade) so the full 122-day archive replays in minutes instead
of hours. Gate constants, candidate ordering, the 30-cap random sample, slot
limits, loss-mint bans, and exit logic are byte-for-byte the engine's.

Usage:
    python3 scripts/walk_forward_replay_extract.py
    python3 scripts/walk_forward_replay_extract.py --start 2026-04-18 --end 2026-08-17
"""

from __future__ import annotations

import argparse
import bisect
import csv
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import duckdb
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = Path(r"/mnt/d/pumpapi-replay")
DEFAULT_OUT = REPO_ROOT / "data" / "walk_forward" / "replay_features.csv"

MAX_AGE_SECONDS = 22 * 60
MAX_OPEN_POSITIONS = 5
POSITION_SIZE_SOL = 0.05
SATURDAY_POSITION_SIZE_SOL = 0.025
BLOCKED_HOURS = {0, 7, 19, 20, 21}
BLOCKED_WEEKDAYS = {2}
BAR_BATCH_SIZE = 100_000

FRICTION_SLIPPAGE_PCT = 3.0
FRICTION_SIZE_SOL = 0.05

MIN_VOLUME_USD = 500.0
MIN_TXNS = 3

FEATURE_FIELDS = [
    "mint", "token_name", "entry_time", "exit_time", "entry_price", "exit_price",
    "exit_reason", "seconds_held", "pool_sol", "pnl_sol", "pnl_pct", "win",
    "entry_date", "exit_date", "position_size_sol",
    "age_minutes", "mcap_usd", "volume_usd", "buy_volume_sol", "sell_volume_sol",
    "buy_sell_ratio", "txns_total", "vol_mcap_ratio", "top10_holder_pct",
    "creator_holdings_pct", "creator_is_selling", "unique_traders",
    "graduated_this_bar", "score_proxy", "score_v1",
]


@dataclass(slots=True)
class RunningStats:
    buy_volume_sol: float = 0.0
    sell_volume_sol: float = 0.0
    trade_count: int = 0
    last_bar_time: int = 0


@dataclass(slots=True)
class Candidate:
    mint: str
    token_name: str | None
    scan_time: int
    ordinal: int
    features: dict[str, Any]


@dataclass(slots=True)
class Bar:
    time: int
    open: float
    high: float
    low: float
    close: float
    market_cap_usd: float | None
    sol_in_pool: float | None


@dataclass(slots=True)
class Trade:
    mint: str
    token_name: str | None
    entry_time: int
    entry_price: float
    exit_time: int
    exit_price: float
    exit_reason: str
    position_size_sol: float
    sol_in_pool_at_entry: float | None
    sol_in_pool_at_exit: float | None
    mcap_usd_at_entry: float | None
    features: dict[str, Any]

    @property
    def pnl_pct(self) -> float:
        return (self.exit_price / self.entry_price - 1.0) * 100.0

    @property
    def pnl_sol(self) -> float:
        return self.position_size_sol * self.pnl_pct / 100.0

    @property
    def seconds_held(self) -> float:
        return (self.exit_time - self.entry_time) / 1000.0


@dataclass(slots=True)
class ScheduledPosition:
    mint: str
    trade: Trade | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--start", help="First replay date YYYY-MM-DD (default: archive start).")
    parser.add_argument("--end", help="Last replay date YYYY-MM-DD (default: archive end).")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    return parser.parse_args()


def finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def positive_number(value: Any) -> float | None:
    number = finite_number(value)
    return number if number is not None and number > 0 else None


def utc_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, UTC)


def iso_time(timestamp_ms: int) -> str:
    return utc_datetime(timestamp_ms).isoformat().replace("+00:00", "Z")


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


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
    """MT-591 score_proxy: replica of run_strategy_b._candidate_strength_score."""
    vol_ratio = vol_usd / mcap_usd if mcap_usd and mcap_usd > 0 else 0.0
    min_txns = max(age_adjusted_min_txns(age_minutes), 1)
    score = 0.0
    score += min(bs_ratio / 2.0, 1.0) * 40.0
    score += min(vol_ratio / 0.05, 1.0) * 30.0
    score += min(txns / (4.0 * min_txns), 1.0) * 15.0
    score += min(vol_usd / (10.0 * MIN_VOLUME_USD), 1.0) * 15.0
    return round(score, 1)


def snapshot_features(
    *,
    mcap_usd: float | None,
    age_seconds: float | None,
    cum_buy_sol: float | None,
    cum_sell_sol: float | None,
    cum_trade_count: float | None,
    sol_usd: float,
    top10_holder_pct: float | None,
    creator_holdings_pct: float | None,
    creator_is_selling: bool | None,
    unique_traders: float | None,
    graduated_this_bar: bool | None,
) -> dict[str, Any]:
    mcap = finite_number(mcap_usd)
    age_s = finite_number(age_seconds)
    buy = finite_number(cum_buy_sol)
    sell = finite_number(cum_sell_sol)
    txns = int(finite_number(cum_trade_count) or 0) if finite_number(cum_trade_count) is not None else None
    volume_usd = (buy + sell) * sol_usd if buy is not None and sell is not None else None
    bs_ratio = buy / sell if buy is not None and sell not in (None, 0) else (float("inf") if buy not in (None, 0) and sell == 0 else None)
    age_min = age_s / 60.0 if age_s is not None else None
    vol_mcap = volume_usd / mcap if volume_usd is not None and mcap not in (None, 0) else None

    score_proxy = None
    if None not in (age_min, bs_ratio, volume_usd, mcap, txns) and age_min is not None:
        score_proxy = strength_score_proxy(bs_ratio, volume_usd, mcap, txns, age_min)
    score_v1 = None
    if age_s is not None and age_s > 0 and buy is not None and sell not in (None, 0):
        rate = (buy + sell) / age_s
        score_v1 = rate * (finite_number(unique_traders) or 0.0) * (buy / sell)

    return {
        "age_minutes": round(age_min, 4) if age_min is not None else None,
        "mcap_usd": round(mcap, 4) if mcap is not None else None,
        "volume_usd": round(volume_usd, 6) if volume_usd is not None else None,
        "buy_volume_sol": round(buy, 6) if buy is not None else None,
        "sell_volume_sol": round(sell, 6) if sell is not None else None,
        "buy_sell_ratio": round(bs_ratio, 6) if bs_ratio is not None and math.isfinite(bs_ratio) else None,
        "txns_total": txns,
        "vol_mcap_ratio": round(vol_mcap, 8) if vol_mcap is not None else None,
        "top10_holder_pct": round(finite_number(top10_holder_pct), 4) if finite_number(top10_holder_pct) is not None else None,
        "creator_holdings_pct": round(finite_number(creator_holdings_pct), 4) if finite_number(creator_holdings_pct) is not None else None,
        "creator_is_selling": bool(creator_is_selling) if creator_is_selling is not None else None,
        "unique_traders": int(finite_number(unique_traders) or 0) if finite_number(unique_traders) is not None else None,
        "graduated_this_bar": bool(graduated_this_bar) if graduated_this_bar is not None else None,
        "score_proxy": score_proxy,
        "score_v1": round(score_v1, 6) if score_v1 is not None and math.isfinite(score_v1) else None,
    }


def open_duckdb(root: Path) -> duckdb.DuckDBPyConnection:
    temporary_dir = root / "derived" / ".stratb-replay-duckdb-tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute(f"SET temp_directory = '{sql_path(temporary_dir)}'")
    connection.execute("SET threads = 2")
    connection.execute("SET preserve_insertion_order = true")
    return connection


def parquet_dates(enriched_dir: Path, start: str | None, end: str | None) -> list[str]:
    available = [path.stem for path in sorted(enriched_dir.glob("*.parquet"))]
    if start is not None or end is not None:
        start = start or available[0]
        end = end or available[-1]
        if start > end:
            raise ValueError(f"--start {start} is after --end {end}")
        return [date for date in available if start <= date <= end]
    return available


def load_sol_prices(derived_dir: Path) -> dict[str, float]:
    csv_path = derived_dir / "sol_prices.csv"
    prices: dict[str, float] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            price = positive_number(row.get("sol_usd"))
            if row.get("date") and price is not None:
                prices[row["date"][:10]] = price
    return prices


def day_rows(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
) -> Iterator[tuple[Any, ...]]:
    source = sql_path(path)
    reader = connection.execute(
        f"""WITH numbered AS (
                SELECT *, row_number() OVER () AS physical_ordinal
                FROM read_parquet('{source}')
            ), running AS (
                SELECT *,
                    sum(coalesce(buy_volume_sol, 0)) OVER mint_window AS cumulative_buy_sol,
                    sum(coalesce(sell_volume_sol, 0)) OVER mint_window AS cumulative_sell_sol,
                    sum(coalesce(trade_count, 0)) OVER mint_window AS cumulative_trade_count
                FROM numbered
                WINDOW mint_window AS (
                    PARTITION BY mint ORDER BY bar_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            )
            SELECT mint, token_name, bar_time, physical_ordinal,
                   cumulative_buy_sol, cumulative_sell_sol, cumulative_trade_count,
                   seconds_since_birth, market_cap_usd,
                   mint_authority_present, freeze_authority_present, creator_holdings_pct,
                   top10_holder_pct, unique_traders, graduated_this_bar, creator_is_selling
            FROM running
            WHERE seconds_since_birth BETWEEN 0 AND {MAX_AGE_SECONDS}
              AND market_cap_usd BETWEEN 5000 AND 50000
              AND mint_authority_present IS FALSE
              AND freeze_authority_present IS FALSE
              AND creator_holdings_pct <= 10
            ORDER BY bar_time, physical_ordinal"""
    ).to_arrow_reader(BAR_BATCH_SIZE)
    for batch in reader:
        columns = [batch.column(index).to_pylist() for index in range(len(batch.schema.names))]
        yield from zip(*columns, strict=True)


def passes_gates(
    *,
    stats: RunningStats,
    timestamp_ms: int,
    age_seconds: float | None,
    market_cap_usd: float | None,
    mint_authority_present: Any,
    freeze_authority_present: Any,
    creator_holdings_pct: float | None,
    sol_usd: float,
) -> bool:
    if age_seconds is None or not 0 <= age_seconds <= MAX_AGE_SECONDS:
        return False
    if market_cap_usd is None or not 5_000 <= market_cap_usd <= 50_000:
        return False
    if stats.trade_count < age_adjusted_min_txns(age_seconds / 60.0):
        return False

    cumulative_volume_usd = (stats.buy_volume_sol + stats.sell_volume_sol) * sol_usd
    if cumulative_volume_usd < 500:
        return False
    if not 0.005 <= cumulative_volume_usd / market_cap_usd <= 50.0:
        return False
    if stats.sell_volume_sol <= 0 or stats.buy_volume_sol / stats.sell_volume_sol < 0.5:
        return False
    if mint_authority_present is not False or freeze_authority_present is not False:
        return False
    if creator_holdings_pct is None or creator_holdings_pct > 10:
        return False

    current_time = utc_datetime(timestamp_ms)
    return current_time.hour not in BLOCKED_HOURS and current_time.weekday() not in BLOCKED_WEEKDAYS


def candidates_for_day(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    sol_usd: float,
    running_stats: dict[str, RunningStats],
) -> tuple[list[Candidate], tuple[str, RunningStats] | None]:
    candidates: list[Candidate] = []
    stats_sample: tuple[str, RunningStats] | None = None
    for row in day_rows(connection, path):
        (
            mint,
            token_name,
            bar_time,
            ordinal,
            cumulative_buy_sol,
            cumulative_sell_sol,
            cumulative_trade_count,
            seconds_since_birth,
            market_cap_usd,
            mint_authority_present,
            freeze_authority_present,
            creator_holdings_pct,
            top10_holder_pct,
            unique_traders,
            graduated_this_bar,
            creator_is_selling,
        ) = row
        timestamp_ms = int(bar_time)
        mint_text = str(mint)
        carry = running_stats.get(mint_text, RunningStats())
        stats = RunningStats(
            buy_volume_sol=carry.buy_volume_sol + (finite_number(cumulative_buy_sol) or 0.0),
            sell_volume_sol=carry.sell_volume_sol + (finite_number(cumulative_sell_sol) or 0.0),
            trade_count=carry.trade_count + int(finite_number(cumulative_trade_count) or 0),
            last_bar_time=timestamp_ms,
        )
        if stats_sample is None:
            stats_sample = (mint_text, stats)

        if passes_gates(
            stats=stats,
            timestamp_ms=timestamp_ms,
            age_seconds=finite_number(seconds_since_birth),
            market_cap_usd=finite_number(market_cap_usd),
            mint_authority_present=mint_authority_present,
            freeze_authority_present=freeze_authority_present,
            creator_holdings_pct=finite_number(creator_holdings_pct),
            sol_usd=sol_usd,
        ):
            features = snapshot_features(
                mcap_usd=market_cap_usd,
                age_seconds=seconds_since_birth,
                cum_buy_sol=stats.buy_volume_sol,
                cum_sell_sol=stats.sell_volume_sol,
                cum_trade_count=stats.trade_count,
                sol_usd=sol_usd,
                top10_holder_pct=top10_holder_pct,
                creator_holdings_pct=creator_holdings_pct,
                creator_is_selling=creator_is_selling,
                unique_traders=unique_traders,
                graduated_this_bar=graduated_this_bar,
            )
            candidates.append(
                Candidate(
                    mint_text,
                    str(token_name) if token_name else None,
                    timestamp_ms,
                    int(ordinal),
                    features,
                )
            )
    return candidates, stats_sample


def carry_stats_for_next_day(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    day_end: int,
) -> dict[str, RunningStats]:
    rows = connection.execute(
        f"""SELECT mint, sum(coalesce(buy_volume_sol, 0)), sum(coalesce(sell_volume_sol, 0)),
                   sum(coalesce(trade_count, 0)), max(bar_time)
            FROM read_parquet('{sql_path(path)}')
            WHERE bar_time >= ?
            GROUP BY mint""",
        [day_end - MAX_AGE_SECONDS * 1000],
    ).fetchall()
    return {
        str(mint): RunningStats(
            buy_volume_sol=finite_number(buy_volume_sol) or 0.0,
            sell_volume_sol=finite_number(sell_volume_sol) or 0.0,
            trade_count=int(finite_number(trade_count) or 0),
            last_bar_time=int(last_bar_time),
        )
        for mint, buy_volume_sol, sell_volume_sol, trade_count, last_bar_time in rows
    }


def pool_sol(minimum: Any, maximum: Any) -> float | None:
    return positive_number(maximum) or positive_number(minimum)


def day_bars_for_mints(
    connection: duckdb.DuckDBPyConnection,
    paths: list[Path],
    mints: list[str],
    window_start: int,
    window_end: int,
) -> dict[str, dict[str, np.ndarray]]:
    """One batched query per day: all OHLCV bars for candidate mints."""
    if not mints:
        return {}
    sources = ", ".join(f"'{sql_path(path)}'" for path in paths)
    placeholders = ", ".join("?" for _ in mints)
    frame = connection.execute(
        f"""SELECT mint, bar_time, open, high, low, close, max_sol_in_pool, market_cap_usd
            FROM read_parquet([{sources}])
            WHERE mint IN ({placeholders}) AND bar_time > ? AND bar_time <= ?
            ORDER BY mint, bar_time""",
        [*mints, window_start, window_end],
    ).fetchdf()
    if frame.empty:
        return {}
    by_mint: dict[str, dict[str, np.ndarray]] = {}
    mint_values = frame["mint"].astype(str).to_numpy()
    split = np.flatnonzero(mint_values[1:] != mint_values[:-1]) + 1
    for mint, start, stop in zip(
        (mint_values[idx] for idx in np.r_[0, split]),
        np.r_[0, split],
        np.r_[split, len(frame)],
        strict=False,
    ):
        by_mint[mint] = {
            "time": frame["bar_time"].to_numpy()[start:stop].astype(np.int64),
            "open": frame["open"].to_numpy()[start:stop],
            "high": frame["high"].to_numpy()[start:stop],
            "low": frame["low"].to_numpy()[start:stop],
            "close": frame["close"].to_numpy()[start:stop],
            "pool": frame["max_sol_in_pool"].to_numpy()[start:stop],
            "mcap": frame["market_cap_usd"].to_numpy()[start:stop],
        }
    return by_mint


def next_bar(bars: dict[str, np.ndarray], scan_time: int) -> Bar | None:
    index = bisect.bisect_right(bars["time"], scan_time)
    if index >= len(bars["time"]):
        return None
    return Bar(
        time=int(bars["time"][index]),
        open=float(bars["open"][index]),
        high=float(bars["high"][index]),
        low=float(bars["low"][index]),
        close=float(bars["close"][index]),
        market_cap_usd=finite_number(bars["mcap"][index]),
        sol_in_pool=pool_sol(None, bars["pool"][index]),
    )


def exit_trade(bars: dict[str, np.ndarray], candidate: Candidate, entry: Bar) -> Trade | None:
    start = bisect.bisect_left(bars["time"], entry.time)
    window_end = entry.time + 10 * 60 * 1000
    position_size = (
        SATURDAY_POSITION_SIZE_SOL
        if utc_datetime(candidate.scan_time).weekday() == 5
        else POSITION_SIZE_SOL
    )
    highest_close = entry.open
    for index in range(start, len(bars["time"])):
        bar_time = int(bars["time"][index])
        if bar_time > window_end:
            break
        bar = Bar(
            time=bar_time,
            open=float(bars["open"][index]),
            high=float(bars["high"][index]),
            low=float(bars["low"][index]),
            close=float(bars["close"][index]),
            market_cap_usd=finite_number(bars["mcap"][index]),
            sol_in_pool=pool_sol(None, bars["pool"][index]),
        )
        if (bar.time - entry.time) % 30_000 >= 5_000:
            continue
        highest_close = max(highest_close, bar.close)
        if bar.close >= entry.open * 2.5:
            return Trade(
                candidate.mint, candidate.token_name, entry.time, entry.open, bar.time,
                entry.open * 2.5, "tp", position_size, entry.sol_in_pool,
                bar.sol_in_pool, entry.market_cap_usd, candidate.features,
            )
        if bar.close <= entry.open * 0.92:
            return Trade(
                candidate.mint, candidate.token_name, entry.time, entry.open, bar.time,
                entry.open * 0.92, "hard_stop", position_size, entry.sol_in_pool,
                bar.sol_in_pool, entry.market_cap_usd, candidate.features,
            )
        if highest_close >= entry.open * 1.02 and bar.close <= highest_close * 0.98:
            return Trade(
                candidate.mint, candidate.token_name, entry.time, entry.open, bar.time,
                highest_close * 0.98, "trailing", position_size, entry.sol_in_pool,
                bar.sol_in_pool, entry.market_cap_usd, candidate.features,
            )
        if bar.time - entry.time >= 10 * 60 * 1000:
            return Trade(
                candidate.mint, candidate.token_name, entry.time, entry.open, bar.time,
                bar.close, "time_stop", position_size, entry.sol_in_pool,
                bar.sol_in_pool, entry.market_cap_usd, candidate.features,
            )
    return None


def settle_positions(
    positions: list[ScheduledPosition],
    through_time: int,
    trades: list[Trade],
    loss_mints: set[str],
) -> list[ScheduledPosition]:
    active: list[ScheduledPosition] = []
    for position in positions:
        if position.trade is not None and position.trade.exit_time <= through_time:
            trades.append(position.trade)
            if position.trade.pnl_sol < 0:
                loss_mints.add(position.mint)
        else:
            active.append(position)
    return active


def replay(
    dates: list[str],
    all_dates: list[str],
    root: Path,
    enriched_dir: Path,
    sol_prices: dict[str, float],
) -> tuple[list[Trade], int]:
    date_indices = {date: index for index, date in enumerate(all_dates)}
    connection = open_duckdb(root)
    running_stats: dict[str, RunningStats] = {}
    positions: list[ScheduledPosition] = []
    loss_mints: set[str] = set()
    trades: list[Trade] = []

    try:
        for days_complete, replay_date in enumerate(dates, start=1):
            sol_usd = sol_prices.get(replay_date)
            if sol_usd is None:
                raise RuntimeError(f"No SOL/USD daily price available for {replay_date}")
            path = enriched_dir / f"{replay_date}.parquet"
            candidates, stats_sample = candidates_for_day(connection, path, sol_usd, running_stats)
            selected = 0
            index = date_indices[replay_date]
            price_paths = [path]
            if index + 1 < len(all_dates):
                price_paths.append(enriched_dir / f"{all_dates[index + 1]}.parquet")
            incomplete_mints: set[str] = set()
            day_end = int(datetime.fromisoformat(replay_date).replace(tzinfo=UTC).timestamp() * 1000) + 86_400_000

            scan_times: dict[int, list[Candidate]] = {}
            for candidate in candidates:
                scan_times.setdefault(candidate.scan_time, []).append(candidate)
            if scan_times:
                bars_by_mint = day_bars_for_mints(
                    connection,
                    price_paths,
                    sorted({candidate.mint for candidates_at_scan in scan_times.values() for candidate in candidates_at_scan}),
                    min(scan_times),
                    max(scan_times) + 10 * 60 * 1000 + 5 * 1000,
                )
            else:
                bars_by_mint = {}

            for scan_time in sorted(scan_times):
                same_bar_candidates = scan_times[scan_time]
                positions = settle_positions(positions, scan_time, trades, loss_mints)
                if len(positions) >= MAX_OPEN_POSITIONS:
                    continue
                candidates_at_scan = list(same_bar_candidates)
                if len(candidates_at_scan) > 30:
                    candidates_at_scan = random.Random(scan_time).sample(candidates_at_scan, 30)
                for candidate in candidates_at_scan:
                    if (
                        candidate.mint in loss_mints
                        or candidate.mint in incomplete_mints
                        or any(pos.mint == candidate.mint for pos in positions)
                        or scan_time > day_end - 10 * 60 * 1000
                    ):
                        continue
                    bars = bars_by_mint.get(candidate.mint)
                    if bars is None:
                        continue
                    entry = next_bar(bars, candidate.scan_time)
                    if entry is None:
                        continue
                    trade = exit_trade(bars, candidate, entry)
                    if trade is None:
                        incomplete_mints.add(candidate.mint)
                        continue
                    positions.append(ScheduledPosition(candidate.mint, trade))
                    selected += 1
                    break

            positions = settle_positions(positions, day_end, trades, loss_mints)
            running_stats = carry_stats_for_next_day(connection, path, day_end)
            if days_complete == 1 and stats_sample is not None:
                mint, stats = stats_sample
                print(
                    f"Stats sanity after day 1: Token {mint}: cumulative trades={stats.trade_count}, "
                    f"buy volume={stats.buy_volume_sol:.6f} SOL, sell volume={stats.sell_volume_sol:.6f} SOL"
                )
            print(
                f"{replay_date}: {len(candidates):,} gate-passing bars, {selected:,} entries, "
                f"{len(positions):,} active/reserved slots, {len(trades):,} settled trades",
                flush=True,
            )
    finally:
        connection.close()

    positions = settle_positions(positions, math.inf, trades, loss_mints)
    return trades, len(positions)


def friction_pnl(trade: Trade) -> float:
    impact = FRICTION_SIZE_SOL / trade.sol_in_pool_at_entry * 100.0 if trade.sol_in_pool_at_entry and trade.sol_in_pool_at_entry > 0 else 0.0
    entry_multiplier = 1 + (FRICTION_SLIPPAGE_PCT + impact) / 100
    exit_multiplier = 1 - FRICTION_SLIPPAGE_PCT / 100
    adjusted_entry = trade.entry_price * entry_multiplier
    adjusted_exit = trade.exit_price * exit_multiplier
    token_amount = FRICTION_SIZE_SOL / adjusted_entry
    proceeds = token_amount * adjusted_exit
    return proceeds - FRICTION_SIZE_SOL


def trade_row(trade: Trade) -> dict[str, Any]:
    pnl_sol = friction_pnl(trade)
    entry_dt = utc_datetime(trade.entry_time)
    exit_dt = utc_datetime(trade.exit_time)
    row = dict(trade.features)
    row.update({
        "mint": trade.mint,
        "token_name": trade.token_name or "",
        "entry_time": iso_time(trade.entry_time),
        "exit_time": iso_time(trade.exit_time),
        "entry_price": trade.entry_price,
        "exit_price": trade.exit_price,
        "exit_reason": trade.exit_reason,
        "seconds_held": trade.seconds_held,
        "pool_sol": trade.sol_in_pool_at_entry if trade.sol_in_pool_at_entry is not None else None,
        "pnl_sol": round(pnl_sol, 8),
        "pnl_pct": round(pnl_sol / FRICTION_SIZE_SOL * 100.0, 6),
        "win": 1 if pnl_sol > 0 else 0,
        "entry_date": entry_dt.date().isoformat(),
        "exit_date": exit_dt.date().isoformat(),
        "position_size_sol": FRICTION_SIZE_SOL,
    })
    return row


def main() -> None:
    args = parse_args()
    root = args.root.resolve()
    enriched_dir = root / "derived" / "enriched"
    all_dates = parquet_dates(enriched_dir, None, None)
    dates = parquet_dates(enriched_dir, args.start, args.end)
    if not dates:
        raise FileNotFoundError(f"No enriched Parquet files found in {enriched_dir}")

    print(f"Replaying {len(dates)} enriched day(s): {dates[0]} through {dates[-1]}", flush=True)
    trades, unclosed_positions = replay(
        dates,
        all_dates,
        root,
        enriched_dir,
        load_sol_prices(root / "derived"),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FEATURE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(trade_row(trade) for trade in trades)

    wins = sum(trade.pnl_sol > 0 for trade in trades)
    total_pnl = sum(friction_pnl(trade) for trade in trades)
    print(
        f"\n{len(trades):,} trades, {wins:,} wins ({wins / len(trades) * 100:.1f}%), "
        f"friction PnL {total_pnl:+.6f} SOL, unclosed positions: {unclosed_positions}",
        flush=True,
    )
    print(f"Wrote {len(trades):,} rows to {args.output}", flush=True)


if __name__ == "__main__":
    main()
