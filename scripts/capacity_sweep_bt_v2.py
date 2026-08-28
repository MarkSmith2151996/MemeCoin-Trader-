#!/usr/bin/env python3
"""MT-678: full-history V2 Strategy BT replay with measured execution costs.

This is deliberately separate from the frozen MT-606/MT-613 scripts. It reads
the enabled V2 configuration from Hive, reproduces the replayable V2 gates,
and runs perfect- and realistic-visibility scenarios in one host-side pass.
It never writes to Hive or starts a runtime service.
"""

# ruff: noqa: E501

from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import heapq
import json
import math
import os
import random
import statistics
import sys
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import duckdb
import numpy as np
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.strategy.position_manager import (  # noqa: E402, I001
    DEX_FEE_PCT,
    PRIORITY_FEE_PER_LEG,
    SLIPPAGE_PCT,
)


DEFAULT_ROOT = Path("/mnt/d/pumpapi-replay")
DEFAULT_OUT_DIR = DEFAULT_ROOT / "results" / "capacity_sweep_bt_v2"
DEFAULT_REPO_REPORT = REPO_ROOT / "data" / "capacity_sweep_bt_v2_report.md"
STRATEGY = "BT"
BAR_BATCH_SIZE = 100_000
ENTRY_DELAY_SECONDS = 42.5
REPEAT_LOSER_BAN_SECONDS = 24 * 60 * 60

# MT-613 calibrated poll model. These are poll-observation parameters rather
# than strategy gates, so they remain separate from Hive's gate configuration.
POLL_SIZE = 30
RECENCY_FLOOR_SECONDS = 120.0
RECENCY_FLOOR_WEIGHT = 100.0
TRADE_WEIGHT = 0.1
TRAILING_BARS = 60

BASELINE_ENTRIES = 282_924
BASELINE_WIN_RATE_PCT = 68.92
BASELINE_FRICTION_PNL_SOL = 1_147.32

REQUIRED_GATES = {
    "mcap_floor",
    "mcap_ceiling",
    "min_age_seconds",
    "max_age_seconds",
    "age_offset_seconds",
    "txn_count_adjustment",
    "min_volume_usd",
    "min_volume_to_mcap_ratio",
    "max_volume_to_mcap_ratio",
    "min_buy_sell_ratio",
    "min_pool_sol_bonding",
    "min_pool_sol_graduated",
    "creator_holdings_max",
    "max_top_holder_pct",
    "score_threshold_bonding",
    "score_threshold_graduated",
    "blocked_weekdays",
    "blocked_hours_utc",
    "max_open",
}
REQUIRED_EXITS = {
    "trailing_stop_pct",
    "trailing_arm_pct",
    "hard_stop_pct",
    "take_profit_pct",
    "time_stop_minutes",
}
KNOWN_EXIT_REASONS = ("take_profit", "trailing_stop", "hard_stop", "time_stop")


@dataclass(frozen=True, slots=True)
class LiveConfig:
    """The V2 strategy configuration captured from Hive at replay start."""

    gates: dict[str, Any]
    exits: dict[str, float]
    position_size_sol: float
    max_open: int
    captured_at: str

    def number(self, name: str) -> float:
        value = self.gates[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuntimeError(f"Hive gate {name} must be numeric")
        return float(value)

    def integers(self, name: str) -> list[int]:
        value = self.gates[name]
        if not isinstance(value, list) or any(isinstance(item, bool) for item in value):
            raise RuntimeError(f"Hive gate {name} must be an integer list")
        try:
            return [int(item) for item in value]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Hive gate {name} must be an integer list") from exc


@dataclass(frozen=True, slots=True)
class Candidate:
    mint: str
    scan_time: int
    ordinal: int
    strength_score: float


@dataclass(frozen=True, slots=True)
class Bar:
    time: int
    open: float
    close: float
    sol_in_pool: float | None


@dataclass(frozen=True, slots=True)
class ReplayTrade:
    mint: str
    entry_time: int
    entry_price: float
    exit_time: int
    exit_price: float
    exit_reason: str
    entry_pool_sol: float
    exit_pool_sol: float
    position_size_sol: float

    @property
    def raw_pnl_sol(self) -> float:
        # A full liquidation cannot withdraw more SOL than the pool holds. This
        # bounds malformed/stale archive marks before reporting pre-cost PnL.
        mark_proceeds = self.position_size_sol * self.exit_price / self.entry_price
        return min(mark_proceeds, self.exit_pool_sol) - self.position_size_sol

    @property
    def net_pnl_sol(self) -> float:
        """Apply measured slippage, AMM impact, DEX fees, and priority fees.

        The token amount is priced at the delayed entry fill plus entry-side
        slippage and constant-product impact. The exit proceeds use the next
    bar's close after the trigger, then the equivalent exit-side costs.
        DEX fees match the per-leg estimate in ``position_manager.py``.
        """

        entry_impact = self.position_size_sol / self.entry_pool_sol
        exit_impact = self.position_size_sol / self.exit_pool_sol
        adjusted_entry = self.entry_price * (1.0 + SLIPPAGE_PCT + entry_impact)
        adjusted_exit = self.exit_price * max(0.0, 1.0 - SLIPPAGE_PCT - exit_impact)
        tokens = self.position_size_sol / adjusted_entry
        proceeds = min(tokens * adjusted_exit, self.exit_pool_sol)
        fees = self.position_size_sol * DEX_FEE_PCT * 2 + PRIORITY_FEE_PER_LEG * 2
        return proceeds - self.position_size_sol - fees


@dataclass(slots=True)
class ScheduledPosition:
    mint: str
    trade: ReplayTrade


@dataclass(slots=True)
class ReplayState:
    scenario: str
    max_open: int
    positions: list[ScheduledPosition] = field(default_factory=list)
    hard_stop_ban_until: dict[str, int] = field(default_factory=dict)
    incomplete_mints: set[str] = field(default_factory=set)
    trades: list[ReplayTrade] = field(default_factory=list)
    entries_signalled: int = 0
    skipped_capacity: int = 0


@dataclass(slots=True)
class RunningStats:
    buy_volume_sol: float = 0.0
    sell_volume_sol: float = 0.0
    trade_count: int = 0
    last_bar_time: int = 0


class VisibilityModel:
    """MT-613's one-way, weighted Jupiter discovery approximation."""

    def __init__(self) -> None:
        self.discovered_at: dict[str, int] = {}
        self.daily_stats: dict[str, dict[str, float | int]] = {}

    @staticmethod
    def _weight(
        cumulative_buy_sol: float,
        cumulative_sell_sol: float,
        cumulative_trades: int,
        age_seconds: float,
    ) -> float:
        weight = (
            1.0
            + math.log1p(max(cumulative_buy_sol + cumulative_sell_sol, 0.0))
            + TRADE_WEIGHT * math.log1p(max(cumulative_trades, 0))
        )
        if age_seconds < RECENCY_FLOOR_SECONDS:
            weight += RECENCY_FLOOR_WEIGHT
        return max(weight, 1e-6)

    @staticmethod
    def _sample(
        rng: random.Random,
        weighted_mints: list[tuple[str, float]],
    ) -> list[str]:
        if len(weighted_mints) <= POLL_SIZE:
            return [mint for mint, _ in weighted_mints]
        # Efraimidis-Spirakis keys are a weighted sample without replacement.
        # It avoids rescanning the whole poll universe once for each of 30 slots.
        choices = heapq.nlargest(
            POLL_SIZE,
            ((rng.random() ** (1.0 / weight), mint) for mint, weight in weighted_mints),
        )
        return [mint for _, mint in choices]

    def simulate_day(
        self,
        rows: Iterator[tuple[Any, ...]],
        replay_date: str,
    ) -> None:
        """Update discovery state and report the day's poll coverage."""

        births: dict[str, int] = {}
        window: dict[int, list[tuple[str, float]]] = {}
        window_order: list[int] = []
        newly_discovered: dict[str, int] = {}
        lags: list[float] = []
        polls = 0
        last_bar: int | None = None

        def poll(current_bar: int) -> None:
            nonlocal polls
            cutoff = current_bar - TRAILING_BARS * 5_000
            while window_order and window_order[0] <= cutoff:
                window.pop(window_order.pop(0), None)
            current: dict[str, float] = {}
            for entries in window.values():
                for mint, weight in entries:
                    current[mint] = weight
            if not current:
                return
            polls += 1
            rng = random.Random(f"mt613:{current_bar}")
            for mint in self._sample(rng, list(current.items())):
                if mint not in self.discovered_at and mint not in newly_discovered:
                    newly_discovered[mint] = current_bar
                    lags.append((current_bar - births[mint]) / 1000.0)

        for row in rows:
            mint, bar_time, buy_sol, sell_sol, trades, age_seconds = row
            mint_text = str(mint)
            bar_ms = int(bar_time)
            age = finite_number(age_seconds) or 0.0
            birth = bar_ms - int(age * 1000)
            births[mint_text] = min(births.get(mint_text, birth), birth)
            if last_bar is not None and bar_ms != last_bar:
                poll(last_bar)
            window.setdefault(bar_ms, []).append(
                (
                    mint_text,
                    self._weight(
                        finite_number(buy_sol) or 0.0,
                        finite_number(sell_sol) or 0.0,
                        int(finite_number(trades) or 0),
                        age,
                    ),
                )
            )
            if not window_order or window_order[-1] != bar_ms:
                window_order.append(bar_ms)
            last_bar = bar_ms
        if last_bar is not None:
            poll(last_bar)

        self.discovered_at.update(newly_discovered)
        day_start = int(datetime.fromisoformat(replay_date).replace(tzinfo=UTC).timestamp() * 1000)
        born_today = [mint for mint, birth in births.items() if day_start <= birth < day_start + 86_400_000]
        lag_sorted = sorted(lags)
        self.daily_stats[replay_date] = {
            "polls": polls,
            "universe_mints": len(births),
            "born_mints": len(born_today),
            "born_discovered": sum(mint in self.discovered_at for mint in born_today),
            "newly_discovered": len(newly_discovered),
            "median_lag_s": statistics.median(lags) if lags else float("nan"),
            "p90_lag_s": lag_sorted[int(len(lag_sorted) * 0.9)] if lags else float("nan"),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--start", default="2026-04-18")
    parser.add_argument("--end", help="Last replay date; default is the latest complete archive day.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--repo-report", type=Path, default=DEFAULT_REPO_REPORT)
    return parser.parse_args()


def finite_number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def positive_number(value: object) -> float | None:
    number = finite_number(value)
    return number if number is not None and number > 0 else None


def utc_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, UTC)


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def age_tier_min_transactions(corrected_age_seconds: float) -> int:
    """Mirror ``services.data_collector._age_adjusted_min_txns`` exactly."""

    if corrected_age_seconds < 60:
        return 3
    if corrected_age_seconds < 180:
        return 5
    if corrected_age_seconds < 300:
        return 8
    if corrected_age_seconds < 600:
        return 12
    return 16


def pool_type(pool: object, graduated_this_bar: object) -> str:
    return "bonding" if str(pool or "").lower() == "pump" and not bool(graduated_this_bar) else "graduated"


def parquet_dates(enriched_dir: Path, start: str, end: str | None) -> list[str]:
    available = sorted(path.stem for path in enriched_dir.glob("*.parquet"))
    if not available:
        raise FileNotFoundError(f"No enriched Parquet files found in {enriched_dir}")
    last_date = end or available[-1]
    if start > last_date:
        raise ValueError(f"--start {start} is after --end {last_date}")
    dates = [date for date in available if start <= date <= last_date]
    if not dates:
        raise FileNotFoundError(f"No replay files in requested range {start} through {last_date}")
    return dates


def load_sol_prices(derived_dir: Path) -> dict[str, float]:
    path = derived_dir / "sol_prices.csv"
    if not path.exists():
        raise FileNotFoundError(f"SOL/USD daily price file is missing: {path}")
    prices: dict[str, float] = {}
    with path.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            price = positive_number(row.get("sol_usd"))
            if row.get("date") and price is not None:
                prices[row["date"][:10]] = price
    return prices


def open_duckdb(root: Path) -> duckdb.DuckDBPyConnection:
    temporary_dir = root / "derived" / ".capacity-sweep-bt-v2-duckdb-tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute(f"SET temp_directory = '{sql_path(temporary_dir)}'")
    connection.execute("SET threads = 2")
    connection.execute("SET preserve_insertion_order = true")
    return connection


async def load_live_config() -> LiveConfig:
    """Read only enabled strategy rows and the local execution-size settings."""

    load_dotenv(REPO_ROOT / ".env", override=False)
    dsn = os.getenv("MEMECOIN_POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("MEMECOIN_POSTGRES_DSN or DATABASE_URL is required")
    position_value = os.getenv("POSITION_SIZE_SOL")
    if position_value is None:
        raise RuntimeError("POSITION_SIZE_SOL must be set in the live environment")
    position_size = positive_number(position_value)
    if position_size is None:
        raise RuntimeError("POSITION_SIZE_SOL must be a positive finite number")

    connection = await asyncpg.connect(dsn)
    try:
        gate_rows = await connection.fetch(
            """
            SELECT gate_name, gate_value::text AS gate_value
            FROM memecoin.gate_config
            WHERE strategy = $1 AND enabled = TRUE
            ORDER BY gate_name
            """,
            STRATEGY,
        )
        exit_rows = await connection.fetch(
            """
            SELECT param_name, param_value
            FROM memecoin.exit_config
            WHERE strategy = $1
            ORDER BY param_name
            """,
            STRATEGY,
        )
    finally:
        await connection.close()

    gates = {str(row["gate_name"]): json.loads(str(row["gate_value"])) for row in gate_rows}
    exits = {str(row["param_name"]): float(row["param_value"]) for row in exit_rows}
    missing_gates = sorted(REQUIRED_GATES - gates.keys())
    missing_exits = sorted(REQUIRED_EXITS - exits.keys())
    if missing_gates or missing_exits:
        details = []
        if missing_gates:
            details.append("missing gates: " + ", ".join(missing_gates))
        if missing_exits:
            details.append("missing exits: " + ", ".join(missing_exits))
        raise RuntimeError("; ".join(details))

    configured_max_open = gates["max_open"]
    if isinstance(configured_max_open, bool) or not isinstance(configured_max_open, (int, float)):
        raise RuntimeError("Hive max_open must be numeric")
    max_open = int(configured_max_open)
    if max_open <= 0:
        raise RuntimeError("Hive max_open must be positive")
    env_max_open = os.getenv("MAX_OPEN")
    if env_max_open is not None and int(env_max_open) != max_open:
        raise RuntimeError(
            f"MAX_OPEN environment value {env_max_open} does not match Hive max_open {max_open}",
        )
    return LiveConfig(
        gates=gates,
        exits=exits,
        position_size_sol=position_size,
        max_open=max_open,
        captured_at=datetime.now(UTC).isoformat(),
    )


def gate_rows(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    config: LiveConfig,
) -> Iterator[tuple[Any, ...]]:
    source = sql_path(path)
    max_age = config.number("max_age_seconds")
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
            SELECT mint, bar_time, physical_ordinal, cumulative_buy_sol, cumulative_sell_sol,
                   cumulative_trade_count, seconds_since_birth, market_cap_usd, max_sol_in_pool,
                   pool, graduated_this_bar, creator_holdings_pct, close
            FROM running
            WHERE seconds_since_birth BETWEEN 0 AND {max_age:.6f}
            ORDER BY bar_time, physical_ordinal""",
    ).to_arrow_reader(BAR_BATCH_SIZE)
    for batch in reader:
        columns = [batch.column(index).to_pylist() for index in range(len(batch.schema.names))]
        yield from zip(*columns, strict=True)


def carry_stats_for_next_day(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    day_end: int,
    max_age_seconds: float,
) -> dict[str, RunningStats]:
    rows = connection.execute(
        f"""SELECT mint, sum(coalesce(buy_volume_sol, 0)), sum(coalesce(sell_volume_sol, 0)),
                   sum(coalesce(trade_count, 0)), max(bar_time)
            FROM read_parquet('{sql_path(path)}')
            WHERE bar_time >= ?
            GROUP BY mint""",
        [day_end - int(max_age_seconds * 1000)],
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


def strength_score(
    *,
    stats: RunningStats,
    corrected_age_seconds: float,
    market_cap_usd: float,
    sol_usd: float,
    config: LiveConfig,
) -> float:
    """Mirror ``services.data_collector._strength_score`` with archive proxies."""

    buy_volume_usd = stats.buy_volume_sol * sol_usd
    sell_volume_usd = stats.sell_volume_sol * sol_usd
    volume_usd = buy_volume_usd + sell_volume_usd
    buy_ratio = buy_volume_usd / max(sell_volume_usd, 1.0)
    volume_mcap_ratio = volume_usd / market_cap_usd
    adjusted_transactions = int(stats.trade_count * config.number("txn_count_adjustment"))
    min_transactions = age_tier_min_transactions(corrected_age_seconds)
    return round(
        min(buy_ratio / 2.0, 1.0) * 40.0
        + min(volume_mcap_ratio / 0.05, 1.0) * 30.0
        + min(adjusted_transactions / (4.0 * min_transactions), 1.0) * 15.0
        + min(volume_usd / (10.0 * config.number("min_volume_usd")), 1.0) * 15.0,
        1,
    )


def candidate_from_row(
    row: tuple[Any, ...],
    *,
    sol_usd: float,
    carry: dict[str, RunningStats],
    config: LiveConfig,
) -> Candidate | None:
    (
        mint,
        bar_time,
        ordinal,
        cumulative_buy_sol,
        cumulative_sell_sol,
        cumulative_trades,
        age_seconds,
        market_cap_usd,
        max_sol_in_pool,
        pool,
        graduated_this_bar,
        creator_holdings_pct,
        close,
    ) = row
    mint_text = str(mint)
    previous = carry.get(mint_text, RunningStats())
    stats = RunningStats(
        buy_volume_sol=previous.buy_volume_sol + (finite_number(cumulative_buy_sol) or 0.0),
        sell_volume_sol=previous.sell_volume_sol + (finite_number(cumulative_sell_sol) or 0.0),
        trade_count=previous.trade_count + int(finite_number(cumulative_trades) or 0),
        last_bar_time=int(bar_time),
    )
    raw_age = finite_number(age_seconds)
    mcap = finite_number(market_cap_usd)
    if raw_age is None or mcap is None:
        return None
    corrected_age = raw_age + config.number("age_offset_seconds")
    if raw_age < config.number("min_age_seconds") or corrected_age > config.number("max_age_seconds"):
        return None
    if not config.number("mcap_floor") <= mcap <= config.number("mcap_ceiling"):
        return None
    current_pool_type = pool_type(pool, graduated_this_bar)
    pool_floor = config.number(
        "min_pool_sol_bonding" if current_pool_type == "bonding" else "min_pool_sol_graduated",
    )
    pool_sol = positive_number(max_sol_in_pool)
    if pool_sol is None or pool_sol < pool_floor:
        return None
    if positive_number(close) is None:
        return None
    buy_volume_usd = stats.buy_volume_sol * sol_usd
    sell_volume_usd = stats.sell_volume_sol * sol_usd
    volume_usd = buy_volume_usd + sell_volume_usd
    if volume_usd < config.number("min_volume_usd"):
        return None
    volume_mcap_ratio = volume_usd / mcap
    if not config.number("min_volume_to_mcap_ratio") <= volume_mcap_ratio <= config.number(
        "max_volume_to_mcap_ratio",
    ):
        return None
    if sell_volume_usd <= 0 or buy_volume_usd / sell_volume_usd < config.number("min_buy_sell_ratio"):
        return None
    if stats.trade_count * config.number("txn_count_adjustment") < age_tier_min_transactions(
        corrected_age,
    ):
        return None
    creator_holdings = finite_number(creator_holdings_pct)
    if creator_holdings is not None and creator_holdings > config.number("creator_holdings_max"):
        return None
    score = strength_score(
        stats=stats,
        corrected_age_seconds=corrected_age,
        market_cap_usd=mcap,
        sol_usd=sol_usd,
        config=config,
    )
    threshold = config.number(
        "score_threshold_bonding" if current_pool_type == "bonding" else "score_threshold_graduated",
    )
    if score < threshold:
        return None
    observed_at = utc_datetime(int(bar_time))
    if observed_at.weekday() in config.integers("blocked_weekdays"):
        return None
    if observed_at.hour in config.integers("blocked_hours_utc"):
        return None
    return Candidate(mint_text, int(bar_time), int(ordinal), score)


def candidates_from_rows(
    rows: Iterator[tuple[Any, ...]],
    sol_usd: float,
    running_stats: dict[str, RunningStats],
    config: LiveConfig,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    for row in rows:
        candidate = candidate_from_row(row, sol_usd=sol_usd, carry=running_stats, config=config)
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def bars_for_mints(
    connection: duckdb.DuckDBPyConnection,
    paths: list[Path],
    mints: list[str],
    window_start: int,
    window_end: int,
) -> dict[str, dict[str, np.ndarray]]:
    if not mints:
        return {}
    sources = ", ".join(f"'{sql_path(path)}'" for path in paths)
    placeholders = ", ".join("?" for _ in mints)
    frame = connection.execute(
        f"""SELECT mint, bar_time, open, close, max_sol_in_pool
            FROM read_parquet([{sources}])
            WHERE mint IN ({placeholders}) AND bar_time >= ? AND bar_time <= ?
            ORDER BY mint, bar_time""",
        [*mints, window_start, window_end],
    ).fetchdf()
    if frame.empty:
        return {}
    result: dict[str, dict[str, np.ndarray]] = {}
    mints_array = frame["mint"].astype(str).to_numpy()
    split = np.flatnonzero(mints_array[1:] != mints_array[:-1]) + 1
    for mint, start, stop in zip(
        (mints_array[index] for index in np.r_[0, split]),
        np.r_[0, split],
        np.r_[split, len(frame)],
        strict=False,
    ):
        result[mint] = {
            "time": frame["bar_time"].to_numpy()[start:stop].astype(np.int64),
            "open": frame["open"].to_numpy()[start:stop],
            "close": frame["close"].to_numpy()[start:stop],
            "pool": frame["max_sol_in_pool"].to_numpy()[start:stop],
        }
    return result


def bar_at(series: dict[str, np.ndarray], index: int) -> Bar | None:
    if index < 0 or index >= len(series["time"]):
        return None
    open_price = positive_number(series["open"][index])
    close_price = positive_number(series["close"][index])
    if open_price is None or close_price is None:
        return None
    return Bar(
        time=int(series["time"][index]),
        open=open_price,
        close=close_price,
        sol_in_pool=positive_number(series["pool"][index]),
    )


def build_trade(
    candidate: Candidate,
    series: dict[str, np.ndarray],
    config: LiveConfig,
) -> ReplayTrade | None:
    """Simulate current exit precedence with delayed next-bar-close fills.

    ``StrategyExecutor._exit_reason`` checks hard stop, take profit, trailing,
    then time stop after updating the position peak. Historical close marks are
    the available proxy for the live mark feed. Every trigger is executed at
    the following completed bar's close, never at the trigger or stop-level
    price. This uses the stable, fully observed 5-second bar price rather than
    the archive's malformed transition-bar opening ticks.
    """

    entry_index = bisect.bisect_left(
        series["time"],
        candidate.scan_time + int(ENTRY_DELAY_SECONDS * 1000),
    )
    entry = bar_at(series, entry_index)
    if entry is None or entry.sol_in_pool is None:
        return None
    entry_price = entry.close
    peak = entry_price
    trailing_armed = False
    time_limit = entry.time + int(config.exits["time_stop_minutes"] * 60 * 1000)
    for index in range(entry_index, len(series["time"])):
        mark = bar_at(series, index)
        if mark is None:
            continue
        peak = max(peak, mark.close)
        trailing_armed = trailing_armed or peak / entry_price >= (
            1.0 + config.exits["trailing_arm_pct"] / 100.0
        )
        reason: str | None = None
        if mark.close <= entry_price * (1.0 - config.exits["hard_stop_pct"] / 100.0):
            reason = "hard_stop"
        elif mark.close >= entry_price * (1.0 + config.exits["take_profit_pct"] / 100.0):
            reason = "take_profit"
        elif trailing_armed and mark.close <= peak * (1.0 - config.exits["trailing_stop_pct"] / 100.0):
            reason = "trailing_stop"
        elif mark.time >= time_limit:
            reason = "time_stop"
        if reason is None:
            continue
        exit_bar = bar_at(series, index + 1)
        if exit_bar is None:
            return None
        exit_pool = exit_bar.sol_in_pool or entry.sol_in_pool
        return ReplayTrade(
            mint=candidate.mint,
            entry_time=entry.time,
            entry_price=entry_price,
            exit_time=exit_bar.time,
            exit_price=exit_bar.close,
            exit_reason=reason,
            entry_pool_sol=entry.sol_in_pool,
            exit_pool_sol=exit_pool,
            position_size_sol=config.position_size_sol,
        )
    return None


def settle(state: ReplayState, through_time: int) -> None:
    active: list[ScheduledPosition] = []
    for position in state.positions:
        if position.trade.exit_time <= through_time:
            state.trades.append(position.trade)
            if position.trade.exit_reason == "hard_stop":
                state.hard_stop_ban_until[position.mint] = (
                    position.trade.exit_time + REPEAT_LOSER_BAN_SECONDS * 1000
                )
        else:
            active.append(position)
    state.positions = active


def eligible(candidate: Candidate, state: ReplayState) -> bool:
    return not (
        candidate.mint in state.incomplete_mints
        or candidate.scan_time < state.hard_stop_ban_until.get(candidate.mint, 0)
        or any(position.mint == candidate.mint for position in state.positions)
    )


def process_candidates(
    candidates: list[Candidate],
    *,
    state: ReplayState,
    bars: dict[str, dict[str, np.ndarray]],
    config: LiveConfig,
) -> None:
    """Use V2's score-descending candidate order until current capacity fills."""

    for candidate in sorted(candidates, key=lambda item: (-item.strength_score, -item.ordinal)):
        if not eligible(candidate, state):
            continue
        if len(state.positions) >= state.max_open:
            state.skipped_capacity += 1
            break
        series = bars.get(candidate.mint)
        if series is None:
            continue
        trade = build_trade(candidate, series, config)
        if trade is None:
            state.incomplete_mints.add(candidate.mint)
            continue
        state.positions.append(ScheduledPosition(candidate.mint, trade))
        state.entries_signalled += 1


def replay(
    *,
    dates: list[str],
    all_dates: list[str],
    root: Path,
    config: LiveConfig,
) -> tuple[ReplayState, ReplayState, VisibilityModel]:
    enriched_dir = root / "derived" / "enriched"
    sol_prices = load_sol_prices(root / "derived")
    perfect = ReplayState("perfect_visibility", config.max_open)
    realistic = ReplayState("realistic_visibility", config.max_open)
    visibility = VisibilityModel()
    date_indices = {date: index for index, date in enumerate(all_dates)}
    running_stats: dict[str, RunningStats] = {}
    connection = open_duckdb(root)

    try:
        for replay_date in dates:
            sol_usd = sol_prices.get(replay_date)
            if sol_usd is None:
                raise RuntimeError(f"No SOL/USD price available for {replay_date}")
            path = enriched_dir / f"{replay_date}.parquet"
            # One cumulative-window query feeds both MT-613 discovery and V2
            # gate evaluation. Keeping the rows for one day avoids a second
            # expensive full-day parquet scan without exposing future bars.
            rows = list(gate_rows(connection, path, config))
            visibility.simulate_day(
                (
                    (row[0], row[1], row[3], row[4], row[5], row[6])
                    for row in rows
                ),
                replay_date,
            )
            perfect_candidates = candidates_from_rows(
                iter(rows),
                sol_usd,
                running_stats,
                config,
            )
            realistic_candidates = [
                candidate
                for candidate in perfect_candidates
                if visibility.discovered_at.get(candidate.mint, math.inf) <= candidate.scan_time
            ]
            scan_times = sorted({candidate.scan_time for candidate in perfect_candidates})
            day_end = int(
                datetime.fromisoformat(replay_date).replace(tzinfo=UTC).timestamp() * 1000
            ) + 86_400_000
            index = date_indices[replay_date]
            price_paths = [path]
            if index + 1 < len(all_dates):
                price_paths.append(enriched_dir / f"{all_dates[index + 1]}.parquet")
            if scan_times:
                max_window_end = max(scan_times) + int(
                    (ENTRY_DELAY_SECONDS + config.exits["time_stop_minutes"] * 60 + 5) * 1000,
                )
                bars = bars_for_mints(
                    connection,
                    price_paths,
                    sorted({candidate.mint for candidate in perfect_candidates}),
                    min(scan_times),
                    max_window_end,
                )
            else:
                bars = {}
            perfect_by_time: dict[int, list[Candidate]] = {}
            realistic_by_time: dict[int, list[Candidate]] = {}
            for candidate in perfect_candidates:
                perfect_by_time.setdefault(candidate.scan_time, []).append(candidate)
            for candidate in realistic_candidates:
                realistic_by_time.setdefault(candidate.scan_time, []).append(candidate)
            for scan_time in scan_times:
                settle(perfect, scan_time)
                settle(realistic, scan_time)
                process_candidates(
                    perfect_by_time[scan_time],
                    state=perfect,
                    bars=bars,
                    config=config,
                )
                process_candidates(
                    realistic_by_time.get(scan_time, []),
                    state=realistic,
                    bars=bars,
                    config=config,
                )
            settle(perfect, day_end)
            settle(realistic, day_end)
            running_stats = carry_stats_for_next_day(
                connection,
                path,
                day_end,
                config.number("max_age_seconds"),
            )
            print(
                f"{replay_date}: gate bars={len(perfect_candidates):,} "
                f"visible={len(realistic_candidates):,} "
                f"perfect trades={len(perfect.trades):,} realistic trades={len(realistic.trades):,}",
                flush=True,
            )
    finally:
        connection.close()
    settle(perfect, math.inf)
    settle(realistic, math.inf)
    return perfect, realistic, visibility


def daily_aggregates(
    trades: list[ReplayTrade],
    dates: list[str],
) -> list[dict[str, Any]]:
    by_day: dict[str, list[ReplayTrade]] = {date: [] for date in dates}
    for trade in trades:
        date = utc_datetime(trade.exit_time).date().isoformat()
        if date in by_day:
            by_day[date].append(trade)
    rows: list[dict[str, Any]] = []
    for date in dates:
        day_trades = by_day[date]
        reason_counts = Counter(trade.exit_reason for trade in day_trades)
        rows.append(
            {
                "date": date,
                "entries": len(day_trades),
                "raw_pnl_sol": sum(trade.raw_pnl_sol for trade in day_trades),
                "net_pnl_sol": sum(trade.net_pnl_sol for trade in day_trades),
                "take_profit_count": reason_counts["take_profit"],
                "trailing_stop_count": reason_counts["trailing_stop"],
                "hard_stop_count": reason_counts["hard_stop"],
                "time_stop_count": reason_counts["time_stop"],
                "other_exit_count": sum(
                    count for reason, count in reason_counts.items() if reason not in KNOWN_EXIT_REASONS
                ),
            },
        )
    return rows


def summary(state: ReplayState, dates: list[str]) -> dict[str, Any]:
    trades = state.trades
    raw_values = [trade.raw_pnl_sol for trade in trades]
    net_values = [trade.net_pnl_sol for trade in trades]
    daily = daily_aggregates(trades, dates)
    raw_days = [row["raw_pnl_sol"] for row in daily]
    net_days = [row["net_pnl_sol"] for row in daily]
    worst_raw = min(daily, key=lambda row: row["raw_pnl_sol"])
    worst_net = min(daily, key=lambda row: row["net_pnl_sol"])
    return {
        "scenario": state.scenario,
        "entries": len(trades),
        "entries_signalled": state.entries_signalled,
        "skipped_capacity": state.skipped_capacity,
        "raw_pnl_sol": sum(raw_values),
        "net_pnl_sol": sum(net_values),
        "raw_win_rate_pct": sum(value > 0 for value in raw_values) / len(trades) * 100 if trades else 0.0,
        "net_win_rate_pct": sum(value > 0 for value in net_values) / len(trades) * 100 if trades else 0.0,
        "raw_daily_mean_sol": statistics.mean(raw_days),
        "raw_daily_median_sol": statistics.median(raw_days),
        "net_daily_mean_sol": statistics.mean(net_days),
        "net_daily_median_sol": statistics.median(net_days),
        "raw_worst_day_sol": worst_raw["raw_pnl_sol"],
        "raw_worst_day_date": worst_raw["date"],
        "net_worst_day_sol": worst_net["net_pnl_sol"],
        "net_worst_day_date": worst_net["date"],
    }


def exit_breakdown(trades: list[ReplayTrade]) -> list[dict[str, Any]]:
    grouped: dict[str, list[ReplayTrade]] = {}
    for trade in trades:
        grouped.setdefault(trade.exit_reason, []).append(trade)
    total_entries = len(trades)
    order = {reason: index for index, reason in enumerate(KNOWN_EXIT_REASONS)}
    rows: list[dict[str, Any]] = []
    for reason in sorted(grouped, key=lambda item: (order.get(item, len(order)), item)):
        reason_trades = grouped[reason]
        raw_pnl = sum(trade.raw_pnl_sol for trade in reason_trades)
        net_pnl = sum(trade.net_pnl_sol for trade in reason_trades)
        raw_wins = sum(trade.raw_pnl_sol > 0 for trade in reason_trades)
        net_wins = sum(trade.net_pnl_sol > 0 for trade in reason_trades)
        rows.append(
            {
                "exit_reason": reason,
                "count": len(reason_trades),
                "raw_win_rate_pct": raw_wins / len(reason_trades) * 100,
                "net_win_rate_pct": net_wins / len(reason_trades) * 100,
                "net_win_rate_contribution_pp": net_wins / total_entries * 100 if total_entries else 0.0,
                "raw_pnl_sol": raw_pnl,
                "net_pnl_sol": net_pnl,
                "raw_avg_pnl_sol": raw_pnl / len(reason_trades),
                "net_avg_pnl_sol": net_pnl / len(reason_trades),
            },
        )
    return rows


def format_number(value: float) -> str:
    return f"{value:+.6f}"


def build_report(
    *,
    dates: list[str],
    config: LiveConfig,
    summaries: list[dict[str, Any]],
    states: list[ReplayState],
    visibility: VisibilityModel,
) -> str:
    by_scenario = {row["scenario"]: row for row in summaries}
    perfect = by_scenario["perfect_visibility"]
    realistic = by_scenario["realistic_visibility"]
    lines = [
        "# MT-678: V2 Strategy BT Full-History Backtest",
        "",
        f"Replay range: **{dates[0]} through {dates[-1]} UTC** ({len(dates)} complete archive days).",
        f"Hive configuration captured read-only at: `{config.captured_at}`.",
        "",
        "## Auditable live V2 parameters",
        "",
        "| source | parameter | value |",
        "| --- | --- | ---: |",
    ]
    for name in sorted(config.gates):
        value = json.dumps(config.gates[name], separators=(",", ":"))
        lines.append(f"| `memecoin.gate_config` | {name} | `{value}` |")
    for name in sorted(config.exits):
        lines.append(f"| `memecoin.exit_config` | {name} | {config.exits[name]:g} |")
    lines.extend(
        [
            f"| live environment | POSITION_SIZE_SOL | {config.position_size_sol:g} |",
            f"| live configuration | MAX_OPEN | {config.max_open} |",
            "",
            "The age-tier transaction requirement remains executable logic in "
            "`services.data_collector._age_adjusted_min_txns`: 3 / 5 / 8 / 12 / 16 "
            "at corrected ages <1 / <3 / <5 / <10 / >=10 minutes. The replay applies "
            "the Hive `txn_count_adjustment` before that comparison.",
            "",
            "## Execution model",
            "",
            "| component | applied model | source |",
            "| --- | --- | --- |",
            (
                f"| entry delay | {ENTRY_DELAY_SECONDS:g}s; first completed archive bar at or after "
                "the delay fills at its close | MT-594 median; 5-second archive granularity |"
            ),
            (
                f"| entry slippage | {SLIPPAGE_PCT * 100:.3f}% plus position_size / entry_pool "
                "constant-product price impact | MT-594; `src/strategy/position_manager.py` |"
            ),
            (
                f"| exit fill | next completed archive bar close after hard-stop, take-profit, trailing, or time trigger; "
                f"{SLIPPAGE_PCT * 100:.3f}% plus position_size / exit_pool impact | MT-678 execution rule; "
                "`services/executor.py` exit priority |"
            ),
            (
                f"| DEX fee | {DEX_FEE_PCT * 100:.2f}% of position size per leg | "
                "`src/strategy/position_manager.py:DEX_FEE_PCT` |"
            ),
            (
                f"| priority fee | {PRIORITY_FEE_PER_LEG:.4f} SOL per leg ({PRIORITY_FEE_PER_LEG * 2:.4f} SOL round trip) | "
                "`src/strategy/position_manager.py:PRIORITY_FEE_PER_LEG` |"
            ),
            "",
            "Raw PnL uses the delayed entry and delayed next-bar exit prices before costs. Net PnL "
            "uses the resulting token quantity after slippage/AMM impact, subtracts both 1% DEX "
            "legs and both priority fees. Both scenarios cap full-liquidation proceeds at the "
            "contemporaneous exit SOL reserve, so malformed/stale archive marks cannot imply an "
            "impossible withdrawal. Net PnL is not comparable to MT-606's flat 3% model.",
            "",
            "### Interpretation warning",
            "",
            "The mean PnL can be dominated by a small number of extreme archived take-profit and "
            "trailing paths even after the pool-reserve bound. The median net day is the more stable "
            "summary in this archive and may be negative while the mean is positive. These results "
            "describe the archived bar/fill surface, not a validated live-profit forecast; no arbitrary "
            "return cap was added because the archive cannot ground one in measured fill evidence.",
            "",
            "Exit priority was traced through `StrategyExecutor._monitor_position_locked` and "
            "`StrategyExecutor._exit_reason`: peak/arm state updates first, then hard stop, take profit, "
            "trailing stop, and time stop. V2 paper code would otherwise fill fixed levels through "
            "`_paper_exit_price`; this replay intentionally supersedes that fill behavior with the required "
            "next-bar execution model.",
            "",
            "## Full-range results",
            "",
            "| metric | perfect visibility | realistic visibility |",
            "| --- | ---: | ---: |",
        ]
    )
    metrics = [
        ("entries", "entries", ",.0f", ""),
        ("raw win rate", "raw_win_rate_pct", ".2f", "%"),
        ("net win rate", "net_win_rate_pct", ".2f", "%"),
        ("raw PnL (SOL)", "raw_pnl_sol", "+.6f", ""),
        ("net PnL (SOL)", "net_pnl_sol", "+.6f", ""),
        ("raw daily mean (SOL)", "raw_daily_mean_sol", "+.6f", ""),
        ("net daily mean (SOL)", "net_daily_mean_sol", "+.6f", ""),
        ("raw daily median (SOL)", "raw_daily_median_sol", "+.6f", ""),
        ("net daily median (SOL)", "net_daily_median_sol", "+.6f", ""),
        ("raw worst day", "raw_worst_day_sol", "+.6f", ""),
        ("net worst day", "net_worst_day_sol", "+.6f", ""),
        ("capacity-blocked scans", "skipped_capacity", ",.0f", ""),
    ]
    for label, key, specifier, suffix in metrics:
        lines.append(
            f"| {label} | {perfect[key]:{specifier}}{suffix} | "
            f"{realistic[key]:{specifier}}{suffix} |",
        )
    lines.append(
        f"| raw worst-day date | {perfect['raw_worst_day_date']} | {realistic['raw_worst_day_date']} |",
    )
    lines.append(
        f"| net worst-day date | {perfect['net_worst_day_date']} | {realistic['net_worst_day_date']} |",
    )
    for state in states:
        lines.extend(
            [
                "",
                f"## Exit-reason breakdown: {state.scenario.replace('_', ' ')}",
                "",
                "| exit reason | count | raw WR | net WR | net WR contribution | raw PnL | net PnL | raw avg/trade | net avg/trade |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ],
        )
        for row in exit_breakdown(state.trades):
            lines.append(
                f"| {row['exit_reason']} | {row['count']:,} | {row['raw_win_rate_pct']:.2f}% | "
                f"{row['net_win_rate_pct']:.2f}% | {row['net_win_rate_contribution_pp']:.2f}pp | "
                f"{format_number(row['raw_pnl_sol'])} | {format_number(row['net_pnl_sol'])} | "
                f"{format_number(row['raw_avg_pnl_sol'])} | {format_number(row['net_avg_pnl_sol'])} |"
            )
    born = sum(int(stats["born_mints"]) for stats in visibility.daily_stats.values())
    found = sum(int(stats["born_discovered"]) for stats in visibility.daily_stats.values())
    lag_values = [float(stats["median_lag_s"]) for stats in visibility.daily_stats.values()]
    finite_lags = [value for value in lag_values if math.isfinite(value)]
    lines.extend(
        [
            "",
            "## Realistic-visibility model",
            "",
            "| metric | result |",
            "| --- | ---: |",
            f"| poll size | {POLL_SIZE} tokens / 5-second bar |",
            f"| simulated polls | {sum(int(stats['polls']) for stats in visibility.daily_stats.values()):,} |",
            f"| born-token discovery coverage | {found / born * 100 if born else 0:.2f}% |",
            f"| median of daily discovery medians | {statistics.median(finite_lags):.2f}s |",
            "",
            "The realistic pass uses MT-613's weighted 30-token poll, 120-second newborn floor, "
            "and one-way discovery/watch-list model. Perfect visibility evaluates every replayable "
            "gate-passing archive observation.",
            "",
            "## Comparison with MT-606 baseline",
            "",
            "| scenario | entries delta vs 282,924 | raw WR delta vs 68.92% | net PnL delta vs +1,147.32 SOL |",
            "| --- | ---: | ---: | ---: |",
        ],
    )
    for row in summaries:
        lines.append(
            f"| {row['scenario']} | {row['entries'] - BASELINE_ENTRIES:+,} | "
            f"{row['raw_win_rate_pct'] - BASELINE_WIN_RATE_PCT:+.2f}pp | "
            f"{row['net_pnl_sol'] - BASELINE_FRICTION_PNL_SOL:+.6f} |"
        )
    lines.extend(
        [
            "",
            "MT-606 used the older gate snapshot, 0.05 SOL standard / 0.025 SOL Saturday sizing, "
            "a flat 3% entry/exit friction model, fixed-level exits, and ended on 2026-08-17. This "
            "run uses V2's Hive gates, 0.02 SOL no-Saturday-multiplier sizing, 42.5-second delayed "
            "entry, next-bar exits, measured per-leg costs, score ordering, hard-stop-only 24-hour "
            "repeat-loser ban, and four additional archive days. Those gate, scheduling, execution-cost, "
            "and data-range changes jointly explain movement; this is not an apples-to-apples parameter-only delta.",
            "",
            "## Replay limits",
            "",
            "- Historical RugCheck/Jupiter-audit reports are absent. The archive's older authority and "
            "  top-10-holder fields are not equivalent to V2's timestamped `mint_authority_revoked`, "
            "  `freeze_authority_revoked`, and single-holder `top_holder_pct` evidence, so those live "
            "  gates are explicitly omitted rather than assumed to pass.",
            "- The archive supplies aggregate buy/sell SOL volume and total trades, not Jupiter's 1-hour "
            "  buy/sell transaction counts. Dollar volumes are reconstructed with the archive's daily "
            "  SOL/USD series and total transactions use the live 1.24 adjustment.",
            "- Archive close marks are a 5-second proxy for the V2 PumpPortal/Jupiter monitor. It cannot "
            "  reproduce sub-bar marks, mark outages/SLA exits, actual route quotes, failed sells, or "
            "  live quarantines.",
            "- Pool labels approximate V2 first-pool classification: `pool == pump` while not graduated "
            "  is treated as bonding; all other rows are treated as graduated.",
            "- The V2 24-hour repeat-loser behavior is modeled as the persisted hard-stop ban in "
            "  `services.strategy`/`services.store`; non-hard-stop losing exits do not create a ban.",
        ],
    )
    return "\n".join(lines) + "\n"


def write_outputs(
    *,
    output_dir: Path,
    repo_report: Path,
    config: LiveConfig,
    dates: list[str],
    states: list[ReplayState],
    visibility: VisibilityModel,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = [summary(state, dates) for state in states]
    report = build_report(
        dates=dates,
        config=config,
        summaries=summaries,
        states=states,
        visibility=visibility,
    )
    (output_dir / "capacity_sweep_bt_v2_report.md").write_text(report, encoding="utf-8")
    repo_report.parent.mkdir(parents=True, exist_ok=True)
    repo_report.write_text(report, encoding="utf-8")

    with (output_dir / "capacity_sweep_bt_v2_summary.csv").open("w", newline="", encoding="utf-8") as output:
        fields = list(summaries[0])
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summaries)
    with (output_dir / "capacity_sweep_bt_v2_exit_reasons.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        fields = ["scenario", *exit_breakdown(states[0].trades)[0].keys()]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for state in states:
            for row in exit_breakdown(state.trades):
                writer.writerow({"scenario": state.scenario, **row})
    with (output_dir / "capacity_sweep_bt_v2_daily_exit_counts.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        fields = ["scenario", *daily_aggregates(states[0].trades, dates)[0].keys()]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for state in states:
            for row in daily_aggregates(state.trades, dates):
                writer.writerow({"scenario": state.scenario, **row})
    with (output_dir / "capacity_sweep_bt_v2_visibility.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        fields = ["date", "polls", "universe_mints", "born_mints", "born_discovered", "newly_discovered", "median_lag_s", "p90_lag_s"]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for date in sorted(visibility.daily_stats):
            writer.writerow({"date": date, **visibility.daily_stats[date]})
    snapshot = {
        "captured_at": config.captured_at,
        "gates": config.gates,
        "exits": config.exits,
        "position_size_sol": config.position_size_sol,
        "max_open": config.max_open,
        "entry_delay_seconds": ENTRY_DELAY_SECONDS,
        "slippage_pct_per_leg": SLIPPAGE_PCT,
        "dex_fee_pct_per_leg": DEX_FEE_PCT,
        "priority_fee_sol_per_leg": PRIORITY_FEE_PER_LEG,
    }
    (output_dir / "capacity_sweep_bt_v2_config.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("\n" + report, flush=True)


def main() -> None:
    args = parse_args()
    root = args.root.resolve()
    enriched_dir = root / "derived" / "enriched"
    config = asyncio.run(load_live_config())
    all_dates = parquet_dates(enriched_dir, "0000-01-01", None)
    dates = parquet_dates(enriched_dir, args.start, args.end)
    print(
        f"Replaying {len(dates)} complete day(s): {dates[0]} through {dates[-1]} "
        f"with position_size={config.position_size_sol:g} SOL max_open={config.max_open}",
        flush=True,
    )
    perfect, realistic, visibility = replay(
        dates=dates,
        all_dates=all_dates,
        root=root,
        config=config,
    )
    write_outputs(
        output_dir=args.output_dir.resolve(),
        repo_report=args.repo_report.resolve(),
        config=config,
        dates=dates,
        states=[perfect, realistic],
        visibility=visibility,
    )
    print(f"Wrote V2 replay outputs to {args.output_dir.resolve()}", flush=True)


if __name__ == "__main__":
    main()
