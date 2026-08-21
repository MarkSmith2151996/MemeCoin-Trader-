#!/usr/bin/env python3
"""MT-606: replay the MT-605 capacity sweep using current Strategy BT gates.

The replay, candidate ordering, capacity accounting, position sizing, and exits
are intentionally shared with ``capacity_sweep.py``.  Only the historical
representation of ``run_strategy_b.py:screen_coin`` is replaced here.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import capacity_sweep as base

DEFAULT_OUT_DIR = base.DEFAULT_ROOT / "results" / "capacity_sweep_bt"
MIN_MCAP_USD = 5_100.0
MAX_MCAP_USD = 50_000.0
POOL_MIN_SOL_BONDING = 5.0
POOL_MIN_SOL_GRADUATED = 5.0
MIN_SCORE_BONDING_CURVE = 40.0
MIN_SCORE_GRADUATED = 40.0
MIN_VOLUME_USD = 500.0
MIN_VOLUME_TO_MCAP_RATIO = 0.005
MAX_VOLUME_TO_MCAP_RATIO = 50.0
MIN_FEES_SOL_PER_15K_MCAP = 0.3


def day_rows(
    connection: Any,
    path: Path,
) -> Iterator[tuple[Any, ...]]:
    """Load every parquet field needed for the replayable Strategy BT gates."""
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
            SELECT mint, token_name, bar_time, physical_ordinal,
                   cumulative_buy_sol, cumulative_sell_sol, cumulative_trade_count,
                   seconds_since_birth, market_cap_usd, max_sol_in_pool, pool,
                   graduated_this_bar, creator_holdings_pct
            FROM running
            WHERE seconds_since_birth BETWEEN 0 AND {base.MAX_AGE_SECONDS}
              AND market_cap_usd BETWEEN {MIN_MCAP_USD:.0f} AND {MAX_MCAP_USD:.0f}
            ORDER BY bar_time, physical_ordinal"""
    ).to_arrow_reader(base.BAR_BATCH_SIZE)
    for batch in reader:
        columns = [batch.column(index).to_pylist() for index in range(len(batch.schema.names))]
        yield from zip(*columns, strict=True)


def _on_bonding_curve(pool: Any, graduated_this_bar: Any) -> bool:
    """Map the replay's pool label to the live firstPool-based classification."""
    return str(pool or "").lower() == "pump" and not bool(graduated_this_bar)


def _candidate_strength_score(
    stats: base.RunningStats,
    age_seconds: float,
    market_cap_usd: float,
    sol_usd: float,
) -> float:
    """The live composite formula with fields available in the replay archive."""
    buys = stats.buy_volume_sol * sol_usd
    sells = stats.sell_volume_sol * sol_usd
    txns = stats.trade_count
    vol = buys + sells
    mcap = market_cap_usd
    bs_ratio = buys / max(sells, 1.0)
    vol_ratio = vol / mcap if mcap > 0 else 0.0
    min_txns = max(base.age_adjusted_min_txns(age_seconds), 1)
    score = 0.0
    score += min(bs_ratio / 2.0, 1.0) * 40.0
    score += min(vol_ratio / 0.05, 1.0) * 30.0
    score += min(txns / (4.0 * min_txns), 1.0) * 15.0
    score += min(vol / (10.0 * max(MIN_VOLUME_USD, 1.0)), 1.0) * 15.0
    return round(score, 1)


def passes_gates(
    *,
    stats: base.RunningStats,
    timestamp_ms: int,
    age_seconds: float | None,
    market_cap_usd: float | None,
    max_sol_in_pool: Any,
    pool: Any,
    graduated_this_bar: Any,
    creator_holdings_pct: float | None,
    sol_usd: float,
) -> bool:
    """Replay the non-provider gates from ``run_strategy_b.screen_coin``."""
    if age_seconds is None or not 0 <= age_seconds <= base.MAX_AGE_SECONDS:
        return False
    if market_cap_usd is None or not MIN_MCAP_USD <= market_cap_usd <= MAX_MCAP_USD:
        return False

    on_bonding_curve = _on_bonding_curve(pool, graduated_this_bar)
    pool_min_sol = POOL_MIN_SOL_BONDING if on_bonding_curve else POOL_MIN_SOL_GRADUATED
    if (pool_sol := base.positive_number(max_sol_in_pool)) is None or pool_sol < pool_min_sol:
        return False

    cumulative_volume_usd = (stats.buy_volume_sol + stats.sell_volume_sol) * sol_usd
    if stats.trade_count < base.age_adjusted_min_txns(age_seconds):
        return False
    if cumulative_volume_usd < MIN_VOLUME_USD:
        return False
    if (
        not MIN_VOLUME_TO_MCAP_RATIO
        <= cumulative_volume_usd / market_cap_usd
        <= MAX_VOLUME_TO_MCAP_RATIO
    ):
        return False
    if stats.sell_volume_sol <= 0 or stats.buy_volume_sol / stats.sell_volume_sol < 0.5:
        return False

    # screen_coin calculates this warning but deliberately does not block paper entries.
    estimated_fees = stats.trade_count * 0.001
    expected_min_fees = (market_cap_usd / 15_000) * MIN_FEES_SOL_PER_15K_MCAP
    low_fees_warn = estimated_fees < expected_min_fees
    _ = low_fees_warn

    score = _candidate_strength_score(stats, age_seconds, market_cap_usd, sol_usd)
    score_threshold = MIN_SCORE_BONDING_CURVE if on_bonding_curve else MIN_SCORE_GRADUATED
    if score < score_threshold:
        return False

    # Live RugCheck treats a missing creator field as a warning; otherwise zero is required.
    if creator_holdings_pct is not None and creator_holdings_pct > 0:
        return False

    current_time = base.utc_datetime(timestamp_ms)
    return (
        current_time.hour not in base.BLOCKED_HOURS
        and current_time.weekday() not in base.BLOCKED_WEEKDAYS
    )


def candidates_for_day(
    connection: Any,
    path: Path,
    sol_usd: float,
    running_stats: dict[str, base.RunningStats],
) -> list[base.Candidate]:
    candidates: list[base.Candidate] = []
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
            max_sol_in_pool,
            pool,
            graduated_this_bar,
            creator_holdings_pct,
        ) = row
        timestamp_ms = int(bar_time)
        mint_text = str(mint)
        carry = running_stats.get(mint_text, base.RunningStats())
        stats = base.RunningStats(
            buy_volume_sol=carry.buy_volume_sol + (base.finite_number(cumulative_buy_sol) or 0.0),
            sell_volume_sol=carry.sell_volume_sol
            + (base.finite_number(cumulative_sell_sol) or 0.0),
            trade_count=carry.trade_count + int(base.finite_number(cumulative_trade_count) or 0),
            last_bar_time=timestamp_ms,
        )
        if passes_gates(
            stats=stats,
            timestamp_ms=timestamp_ms,
            age_seconds=base.finite_number(seconds_since_birth),
            market_cap_usd=base.finite_number(market_cap_usd),
            max_sol_in_pool=max_sol_in_pool,
            pool=pool,
            graduated_this_bar=graduated_this_bar,
            creator_holdings_pct=base.finite_number(creator_holdings_pct),
            sol_usd=sol_usd,
        ):
            candidates.append(
                base.Candidate(
                    mint_text, str(token_name) if token_name else None, timestamp_ms, int(ordinal)
                )
            )
    return candidates


def _load_mt605_max_open_5() -> dict[str, str]:
    path = base.DEFAULT_ROOT / "results" / "capacity_sweep" / "capacity_sweep_summary.csv"
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row["max_open"] == "5":
                return row
    raise RuntimeError(f"MT-605 MAX_OPEN=5 row missing from {path}")


def build_report(rows: list[dict[str, Any]]) -> str:
    live = next(row for row in rows if row["max_open"] == "5")
    strategy_b = _load_mt605_max_open_5()
    lines = [
        "# MT-606: Strategy BT Concurrent Position Capacity Sweep",
        "",
        (
            "122-day replay (2026-04-18 through 2026-08-17) using the MT-605 replay "
            "and capacity engine."
        ),
        (
            "Strategy BT matches the current replayable `run_strategy_b.py:screen_coin` "
            "gates: $5,100-$50,000 mcap, 5 SOL pool floor, composite score >=40, "
            "transaction/volume/volume-to-mcap/buy-sell gates, creator holdings <=0% "
            "when populated, Wednesday blocked, and UTC dead zones 0/19/20/21."
        ),
        (
            "Exit logic remains 2.5x TP, 8% hard stop, 2% trailing arm/stop, "
            "and a 10-minute time stop."
        ),
        "Position size remains 0.05 SOL (0.025 SOL on Saturday).",
        "",
        "## MAX_OPEN=5 comparison",
        "",
        "| metric | Strategy B (MT-605) | Strategy BT (MT-606) |",
        "| --- | ---: | ---: |",
        f"| entries | {int(strategy_b['entries']):,} | {live['entries']:,} |",
        f"| win rate | {float(strategy_b['win_rate_pct']):.2f}% | {live['win_rate_pct']:.2f}% |",
        f"| raw PnL (SOL) | {float(strategy_b['pnl_sol']):+.6f} | {live['pnl_sol']:+.6f} |",
        (
            f"| friction PnL (SOL) | {float(strategy_b['pnl_friction_sol']):+.6f} "
            f"| {live['pnl_friction_sol']:+.6f} |"
        ),
        (
            f"| daily mean (SOL) | {float(strategy_b['daily_pnl_mean']):+.6f} "
            f"| {live['daily_pnl_mean']:+.6f} |"
        ),
        (
            f"| daily median (SOL) | {float(strategy_b['daily_pnl_median']):+.6f} "
            f"| {live['daily_pnl_median']:+.6f} |"
        ),
        "",
        "## Strategy BT capacity results",
        "",
        (
            "| max_open | entries | skipped | PnL (SOL) | friction PnL | win rate | "
            "daily mean | daily median | avg conc | peak conc |"
        ),
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        pnl_fields = (
            f"{row['pnl_sol']:+.2f} | {row['pnl_friction_sol']:+.2f} | {row['win_rate_pct']:.2f}% |"
        )
        lines.append(
            f"| {row['max_open']} | {row['entries']:,} | {row['skipped_capacity']:,} | "
            f"{pnl_fields} "
            f"{row['daily_pnl_mean']:+.3f} | {row['daily_pnl_median']:+.3f} | "
            f"{row['avg_concurrent']:.2f} | {row['peak_concurrent']} |"
        )
    lines.extend(
        [
            "",
            "## Replay limits",
            "",
            "- RugCheck is skipped because the replay archive has no historical provider reports.",
            "  This omits live authority, top-holder, and provider-risk rejections, so results are",
            "  an optimistic BT capacity ceiling.",
            ("- The archive has no Jupiter `firstPool.id`; its `pool == pump` label approximates"),
            "  bonding-curve status. Both current live pool floors and score thresholds are 5 SOL",
            "  and 40, respectively, so this cannot change the present gate outcome.",
            "- The archive stores aggregate SOL buy/sell volume, not Jupiter buy/sell tx counts.",
            "  counts. The live composite formula is otherwise unchanged, using replay trade_count",
            "  and USD-converted SOL-volume buy/sell ratio as the available historical proxy.",
            "  `unique_wallets_total` exists, but current live score calculation does not use it.",
            "- The live low-fee calculation is `trade_count * 0.001` versus the expected minimum.",
            "  As in current paper-mode `screen_coin`, it is a warning",
            "  only and does not block entry; the live loop does not compare it with actual fees.",
            (
                "- Any live-only API failure, stale-cache, and discovery-order effects are not "
                "replayable from parquets."
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
    with (output_dir / "capacity_sweep_bt_summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(rows)
    with (output_dir / "capacity_sweep_bt_skipped_by_hour.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.writer(output)
        writer.writerow(["max_open", "utc_hour", "skipped_entries"])
        for state in states:
            for hour in sorted(state.skipped_by_hour):
                writer.writerow([base.label(state.max_open), hour, state.skipped_by_hour[hour]])
    report = build_report(rows)
    (output_dir / "capacity_sweep_bt_report.md").write_text(report, encoding="utf-8")
    print("\n" + report)


base.DEFAULT_OUT_DIR = DEFAULT_OUT_DIR
base.day_rows = day_rows
base.candidates_for_day = candidates_for_day
base.write_outputs = write_outputs


if __name__ == "__main__":
    base.main()
