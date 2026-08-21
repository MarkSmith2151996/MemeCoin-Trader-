#!/usr/bin/env python3
"""MT-613: realistic-visibility backtest — simulate Jupiter polling constraints.

The MT-606 replay (``capacity_sweep_bt.py``) assumes perfect visibility: every
token inside the $5.1K-$50K mcap window is evaluated on every 5-second bar.
Live Jupiter does not deliver that.  Its three discovery endpoints
(``/toporganicscore/5m``, ``/recent``, ``/toptrending/5m``) return ~33 unique
tokens per poll (MT-609 funnel: 32.7 avg), so only a fraction of the market is
ever surfaced to the loop at a given instant.

This script keeps every gate, exit, ordering, and capacity rule byte-identical
to ``capacity_sweep_bt.py`` and adds a single visibility filter before gate
evaluation:

1. Each 5-second bar, one simulated Jupiter poll samples ``POLL_SIZE`` tokens
   (30, matching the funnel's 32.7/poll) from the young-token universe (age
   <= 22 min, any mcap), weighted by activity (cumulative buy+sell SOL volume
   and trade count) plus a recency floor for newborns so brand-new tokens can
   be discovered before they accumulate volume.
2. A token must be *discovered* (sampled in a poll at bar t) before any of its
   bars can produce candidates.  Discovery is one-way: once discovered the
   token is on the watch list and follows the exact same continuous
   re-evaluation as MT-606 (a candidate is emitted on every gate-passing bar
   up to the 22-minute age cap) — the MT-610 watch-list semantics.
3. Tokens the poll never surfaces never enter the pipeline — they are
   invisible, same as in live.

The discovery map is carried across days (a token discovered yesterday remains
on the watch list today while still young), mirroring how ``running_stats``
carry.

Poll parameters mirror the MT-609 funnel / MT-613 benchmark measurements:
~33 tokens/poll, one poll per 5s bar (live polls ~1s but consecutive polls
overlap heavily — the funnel's 8,938 dedup skips vs 1,951 unique new mints),
99.3% of born tokens surfaced at least once, median discovery lag ~5s.

Usage:
    python3 scripts/capacity_sweep_bt_realistic.py --max-open 5
    python3 scripts/capacity_sweep_bt_realistic.py --start 2026-08-01 --end 2026-08-17

Outputs (default <root>/results/capacity_sweep_bt_realistic/):
    capacity_sweep_bt_realistic_summary.csv
    capacity_sweep_bt_realistic_report.md   (includes the 3-way comparison)
"""

from __future__ import annotations

import csv
import math
import random
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import capacity_sweep as base
import capacity_sweep_bt as bt

DEFAULT_OUT_DIR = base.DEFAULT_ROOT / "results" / "capacity_sweep_bt_realistic"

POLL_SIZE = 30
POLLS_PER_BAR = 1
RECENCY_FLOOR_S = 120.0
RECENCY_FLOOR_WEIGHT = 100.0
TRADE_WEIGHT = 0.1
TRAILING_BARS = 60  # 5-minute activity window, matching Jupiter's /5m endpoints

LIVE_AUG21_SOL_PER_DAY = 8.19

# mint -> first discovery bar_time (epoch ms). Carried across replay days so a
# token discovered yesterday stays watch-listed today while still young.
_DISCOVERED: dict[str, int] = {}

# Per-day discovery telemetry for the report.
_DISCOVERY_STATS: dict[str, dict[str, Any]] = {}


def _weight(
    cumulative_buy_sol: float,
    cumulative_sell_sol: float,
    cumulative_trade_count: int,
    age_seconds: float,
) -> float:
    """Activity level that determines poll membership (Jupiter's organic score).

    Jupiter's lists are score-ranked (organic score, trending), not raw-volume
    ranked, so the weight uses log1p-scaled cumulative volume — a token with
    10,000 SOL of lifetime volume is only ~2.3x more likely to be surfaced than
    one with 100 SOL, not 100x.  Newborns get a recency floor (the /recent list
    surfaces tokens by creation time regardless of volume — measured 99.3%
    birth coverage with 5.2s median lag).
    """
    volume = cumulative_buy_sol + cumulative_sell_sol
    weight = (
        1.0
        + math.log1p(max(volume, 0.0))
        + TRADE_WEIGHT * math.log1p(max(cumulative_trade_count, 0))
    )
    if age_seconds < RECENCY_FLOOR_S:
        weight += RECENCY_FLOOR_WEIGHT
    return max(weight, 1e-6)


def _weighted_sample(
    rng: random.Random,
    items: list[tuple[str, float]],
    k: int,
) -> list[str]:
    """Sample k distinct items without replacement, proportional to weight."""
    if len(items) <= k:
        return [mint for mint, _ in items]
    picked: list[str] = []
    pool = list(items)
    for _ in range(k):
        total = sum(weight for _, weight in pool)
        target = rng.random() * total
        running = 0.0
        for index, (mint, weight) in enumerate(pool):
            running += weight
            if running >= target:
                picked.append(mint)
                pool.pop(index)
                break
        else:
            picked.append(pool.pop()[0])
    return picked


def poll_rows(
    connection: Any,
    path: Path,
) -> list[tuple[Any, ...]]:
    """All young-token bars (age <= 22 min, any mcap) with cumulative activity.

    The poll universe is everything Jupiter could return — tokens at any mcap,
    not just the $5.1K-$50K evaluation window.  Cumulative stats let a token
    stay in the poll while it is active (matching the funnel's repeat-dominated
    polls) without ever reading future data.
    """
    source = base.sql_path(path)
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
            SELECT mint, bar_time, physical_ordinal,
                   cumulative_buy_sol, cumulative_sell_sol, cumulative_trade_count,
                   seconds_since_birth
            FROM running
            WHERE seconds_since_birth BETWEEN 0 AND {base.MAX_AGE_SECONDS}
            ORDER BY bar_time, physical_ordinal"""
    ).to_arrow_reader(base.BAR_BATCH_SIZE)
    rows: list[tuple[Any, ...]] = []
    for batch in reader:
        columns = [batch.column(index).to_pylist() for index in range(len(batch.schema.names))]
        rows.extend(zip(*columns, strict=True))
    return rows


def simulate_day_polls(connection: Any, path: Path, replay_date: str) -> None:
    """Run one poll per 5s bar and update ``_DISCOVERED`` plus telemetry.

    The poll universe at bar t is every young token with any bar in the
    trailing ``TRAILING_BARS`` window — Jupiter's /5m lists keep a token
    visible for minutes after its last trade, and its /recent list surfaces
    newborns by creation time regardless of volume.  Weights use the token's
    cumulative activity at its most recent bar, so active tokens stay in the
    poll (the funnel's repeat-dominated polls) and newborns ride the recency
    floor.
    """
    universe_rows = poll_rows(connection, path)
    birth_ms: dict[str, int] = {}
    # rolling window: bar_time -> list of (mint, weight) with a bar at bar_time
    window: dict[int, list[tuple[str, float]]] = {}
    window_order: list[int] = []
    discovered_new: dict[str, int] = {}
    lag_seconds: list[float] = []
    last_bar = 0
    polls_run = 0

    def _flush_poll(current_bar: int) -> None:
        """Run the poll for ``current_bar`` against the trailing window."""
        nonlocal window_order, polls_run
        cutoff = current_bar - TRAILING_BARS * 5_000
        while window_order and window_order[0] <= cutoff:
            window.pop(window_order.pop(0), None)
        universe: list[tuple[str, float]] = []
        seen: dict[str, float] = {}
        for mints in window.values():
            for mint, weight in mints:
                seen[mint] = weight
        universe = list(seen.items())
        if not universe:
            return
        polls_run += 1
        rng = random.Random(f"mt613:{current_bar}")
        for _ in range(POLLS_PER_BAR):
            for mint in _weighted_sample(rng, universe, POLL_SIZE):
                if mint not in _DISCOVERED and mint not in discovered_new:
                    discovered_new[mint] = current_bar
                    lag_seconds.append((current_bar - birth_ms[mint]) / 1000.0)

    for row in universe_rows:
        (
            mint,
            bar_time,
            _ordinal,
            cumulative_buy_sol,
            cumulative_sell_sol,
            cumulative_trade_count,
            seconds_since_birth,
        ) = row
        mint_text = str(mint)
        bar_ms = int(bar_time)
        age_s = base.finite_number(seconds_since_birth) or 0.0
        token_birth_ms = bar_ms - int(age_s * 1000)
        if mint_text not in birth_ms or token_birth_ms < birth_ms[mint_text]:
            birth_ms[mint_text] = token_birth_ms
        if last_bar and bar_ms != last_bar:
            _flush_poll(last_bar)
        window.setdefault(bar_ms, []).append(
            (
                mint_text,
                _weight(
                    base.finite_number(cumulative_buy_sol) or 0.0,
                    base.finite_number(cumulative_sell_sol) or 0.0,
                    int(base.finite_number(cumulative_trade_count) or 0),
                    age_s,
                ),
            )
        )
        if not window_order or window_order[-1] != bar_ms:
            window_order.append(bar_ms)
        last_bar = bar_ms
    _flush_poll(last_bar)

    day_start_ms = int(
        datetime.fromisoformat(replay_date).replace(tzinfo=UTC).timestamp() * 1000
    )

    for mint, first_bar in discovered_new.items():
        _DISCOVERED[mint] = first_bar

    born_this_day = {
        mint
        for mint, birth in birth_ms.items()
        if day_start_ms <= birth < day_start_ms + 86_400_000
    }
    lag_sorted = sorted(lag_seconds)
    _DISCOVERY_STATS[replay_date] = {
        "universe_bars": polls_run,
        "unique_universe_mints": len(birth_ms),
        "born_this_day": len(born_this_day),
        "born_this_day_discovered": sum(1 for mint in born_this_day if mint in _DISCOVERED),
        "newly_discovered": len(discovered_new),
        "median_lag_s": (
            lag_sorted[len(lag_sorted) // 2] if lag_sorted else float("nan")
        ),
        "p90_lag_s": (
            lag_sorted[int(len(lag_sorted) * 0.9)] if lag_sorted else float("nan")
        ),
    }


def candidates_for_day(
    connection: Any,
    path: Path,
    sol_usd: float,
    running_stats: dict[str, base.RunningStats],
) -> list[base.Candidate]:
    """MT-606 candidates filtered to tokens visible to the simulated poll.

    Gate evaluation, ordering, and everything else is delegated to
    ``capacity_sweep_bt.candidates_for_day`` unchanged; the only difference is
    that a candidate bar is dropped when the token had not yet been discovered
    by that bar (or was never discovered at all).
    """
    replay_date = path.stem
    simulate_day_polls(connection, path, replay_date)
    visible: list[base.Candidate] = []
    for candidate in bt.candidates_for_day(connection, path, sol_usd, running_stats):
        first_seen = _DISCOVERED.get(candidate.mint)
        if first_seen is not None and first_seen <= candidate.scan_time:
            visible.append(candidate)
    return visible


def _load_mt606_max_open_5() -> dict[str, str]:
    path = base.DEFAULT_ROOT / "results" / "capacity_sweep_bt" / "capacity_sweep_bt_summary.csv"
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row["max_open"] == "5":
                return row
    raise RuntimeError(f"MT-606 MAX_OPEN=5 row missing from {path}")


def build_report(rows: list[dict[str, Any]]) -> str:
    live = next(row for row in rows if row["max_open"] == "5")
    perfect = _load_mt606_max_open_5()
    perfect_daily_mean = float(perfect["daily_pnl_mean"])
    realistic_daily_mean = float(live["daily_pnl_mean"])
    total_born = sum(s.get("born_this_day", 0) for s in _DISCOVERY_STATS.values())
    total_born_found = sum(
        s.get("born_this_day_discovered", 0) for s in _DISCOVERY_STATS.values()
    )
    coverage_pct = total_born_found / total_born * 100.0 if total_born else float("nan")
    lag_medians = [s.get("median_lag_s", float("nan")) for s in _DISCOVERY_STATS.values()]
    lines = [
        "# MT-613: Realistic-Visibility Backtest — Simulate Jupiter Polling Constraints",
        "",
        (
            "122-day replay (2026-04-18 through 2026-08-17) using the MT-605 replay "
            "and capacity engine with MT-606 gates, exits, ordering, and capacity "
            "rules unchanged."
        ),
        "",
        "## The visibility model",
        "",
        (
            "Every 5-second bar, one simulated Jupiter poll samples 30 tokens "
            "(POLL_SIZE, matching the MT-609 funnel's 32.7 unique/poll) from the "
            "young-token universe (age <= 22 min, any mcap), weighted by activity "
            "(cumulative buy+sell SOL volume and trade count) plus a recency floor "
            "for tokens younger than 120s so newborns can be discovered before they "
            "accumulate volume."
        ),
        (
            "A token must be discovered (sampled in a poll) before any of its bars "
            "can produce candidates.  Discovery is one-way — once on the watch list "
            "the token is re-evaluated on every gate-passing bar up to the 22-minute "
            "age cap, exactly as in MT-606 (MT-610 watch-list semantics).  Tokens "
            "the poll never surfaces never enter the pipeline."
        ),
        "",
        "## Measured Jupiter visibility (MT-609 funnel + feed benchmark)",
        "",
        "| metric | measured |",
        "| --- | ---: |",
        "| unique tokens per poll | 32.7 (avg, 3,476 polls/hour) |",
        "| unique new mints surfaced per hour | 1,951 |",
        "| born tokens surfaced at least once (2h benchmark) | 99.3% |",
        "| discovery lag | median 5.2s, p90 7.7s, max 56s |",
        "| poll repeats (funnel dedup skips) | 8,938 events vs 1,951 new |",
        "",
        "## Simulated visibility (this run)",
        "",
        "| metric | simulated |",
        "| --- | ---: |",
        f"| polls simulated | {sum(s['universe_bars'] for s in _DISCOVERY_STATS.values()):,} |",
        (
            "| unique mints in poll universe | "
            f"{sum(s['unique_universe_mints'] for s in _DISCOVERY_STATS.values()):,} |"
        ),
        f"| newly discovered | {sum(s['newly_discovered'] for s in _DISCOVERY_STATS.values()):,} |",
        (
            f"| median discovery lag | "
            f"{statistics.median(lag_medians):.1f}s (daily median, "
            f"measured live: 5.2s) |"
        ),
        (
            f"| daily born-token coverage | "
            f"{coverage_pct:.1f}% (measured live: 99.3%) |"
        ),
        "",
        "## MAX_OPEN=5 results",
        "",
        "| metric | perfect visibility (MT-606) | realistic visibility (MT-613) |",
        "| --- | ---: | ---: |",
        f"| entries | {int(perfect['entries']):,} | {live['entries']:,} |",
        f"| win rate | {float(perfect['win_rate_pct']):.2f}% | {live['win_rate_pct']:.2f}% |",
        f"| raw PnL (SOL) | {float(perfect['pnl_sol']):+.6f} | {live['pnl_sol']:+.6f} |",
        (
            f"| friction PnL (SOL) | {float(perfect['pnl_friction_sol']):+.6f} "
            f"| {live['pnl_friction_sol']:+.6f} |"
        ),
        (
            f"| daily mean (SOL) | {perfect_daily_mean:+.6f} "
            f"| {realistic_daily_mean:+.6f} |"
        ),
        (
            f"| daily median (SOL) | {float(perfect['daily_pnl_median']):+.6f} "
            f"| {live['daily_pnl_median']:+.6f} |"
        ),
        (
            f"| PnL/entry (SOL) | {float(perfect['pnl_per_entry_sol']):+.8f} "
            f"| {live['pnl_per_entry_sol']:+.8f} |"
        ),
        "",
        "## The 3-way comparison",
        "",
        "| scenario | SOL/day (daily mean) | what it means |",
        "| --- | ---: | --- |",
        (
            "| Perfect visibility backtest (MT-606) | "
            f"+{perfect_daily_mean:.2f} | unrealistic ceiling — every token, every bar |"
        ),
        (
            "| Realistic visibility backtest (MT-613, this run) | "
            f"{realistic_daily_mean:+.2f} | the true target if visibility were the only gap |"
        ),
        (
            "| Live loop (Aug 21) | "
            f"+{LIVE_AUG21_SOL_PER_DAY:.2f} | what we are actually getting |"
        ),
        "",
    ]
    if realistic_daily_mean >= 15.0:
        verdict = (
            f"The realistic backtest (+{realistic_daily_mean:.2f} SOL/day) is far above "
            "the live loop's +8.19 SOL/day, so even after Jupiter's polling "
            "constraints are modeled there is still code to fix — visibility is not "
            "the dominant gap."
        )
    elif realistic_daily_mean >= 10.0:
        verdict = (
            f"The realistic backtest (+{realistic_daily_mean:.2f} SOL/day) sits between "
            "the perfect-visibility ceiling and the live loop.  Visibility explains "
            "part of the gap, but some code-side loss remains."
        )
    else:
        verdict = (
            f"The realistic backtest (+{realistic_daily_mean:.2f} SOL/day) lands near "
            "the live loop's +8.19 SOL/day: the loop is performing near its Jupiter-"
            "visibility ceiling, and the remaining gap is structural (Jupiter's API, "
            "not our code)."
        )
    lines.extend(
        [
            "## Verdict",
            "",
            verdict,
            "",
            "## Caveats",
            "",
            (
                "- The poll model samples 30 tokens per 5s bar with cumulative-activity "
                "weighting and a 120s newborn floor; live Jupiter polls ~1s and its "
                "three lists overlap heavily (funnel dedup skips), so one poll per bar "
                "is a conservative simplification of per-cycle unique visibility."
            ),
            (
                "- Discovery is one-way: once discovered, a token is re-evaluated on "
                "every gate-passing bar up to 22 minutes (MT-610 watch-list semantics). "
                "Live MT-612 additionally skips re-screening tokens absent from the "
                "latest poll, so this replay is more generous than the live loop on "
                "re-evaluation frequency."
            ),
            (
                "- RugCheck is skipped because the replay archive has no historical "
                "provider reports, so results remain an optimistic ceiling for the "
                "gates themselves."
            ),
            (
                "- The archive stores aggregate SOL buy/sell volume, not Jupiter "
                "buy/sell tx counts; the live composite score formula is otherwise "
                "unchanged using replay trade_count and USD-converted SOL-volume "
                "buy/sell ratio as the available historical proxy."
            ),
            (
                "- The simulated poll universe is all young tokens in the archive "
                "(any mcap, age <= 22 min); tokens born in earlier parquets are "
                "covered by the discovery carry-over."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def write_outputs(states: list[base.ConfigState], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = base.summary_rows(states)
    summary_fields = [
        "max_open",
        "entries",
        "skipped_capacity",
        "pnl_sol",
        "pnl_friction_sol",
        "win_rate_pct",
        "pnl_per_entry_sol",
        "friction_pnl_per_entry_sol",
        "avg_concurrent",
        "peak_concurrent",
        "daily_pnl_mean",
        "daily_pnl_median",
        "daily_pnl_std",
        "worst_day_sol",
        "worst_day_date",
        "worst_day_trades",
    ]
    with (output_dir / "capacity_sweep_bt_realistic_summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(rows)
    with (output_dir / "capacity_sweep_bt_realistic_skipped_by_hour.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.writer(output)
        writer.writerow(["max_open", "utc_hour", "skipped_entries"])
        for state in states:
            for hour in sorted(state.skipped_by_hour):
                writer.writerow([base.label(state.max_open), hour, state.skipped_by_hour[hour]])
    with (output_dir / "capacity_sweep_bt_realistic_discovery.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            ["date", "universe_bars", "unique_universe_mints", "born_this_day",
             "born_this_day_discovered", "newly_discovered", "median_lag_s", "p90_lag_s"]
        )
        for date in sorted(_DISCOVERY_STATS):
            stats = _DISCOVERY_STATS[date]
            writer.writerow(
                [
                    date,
                    stats["universe_bars"],
                    stats["unique_universe_mints"],
                    stats["born_this_day"],
                    stats["born_this_day_discovered"],
                    stats["newly_discovered"],
                    f"{stats['median_lag_s']:.1f}",
                    f"{stats['p90_lag_s']:.1f}",
                ]
            )
    report = build_report(rows)
    (output_dir / "capacity_sweep_bt_realistic_report.md").write_text(report, encoding="utf-8")
    print("\n" + report)


base.DEFAULT_OUT_DIR = DEFAULT_OUT_DIR
base.day_rows = bt.day_rows
base.candidates_for_day = candidates_for_day
base.write_outputs = write_outputs


if __name__ == "__main__":
    base.main()
