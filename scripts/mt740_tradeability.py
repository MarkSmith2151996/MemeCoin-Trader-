#!/usr/bin/env python3
"""MT-740: test whether MT-737 graduate features are usable by the BT gate.

The worker mode processes one-day gate and five-day outcome partitions in
subprocesses.
It reads no file dated May 18, 2026 or later and writes only detached analysis
artifacts; it does not import or modify the replay engine.
"""

from __future__ import annotations

import argparse
import csv
import math
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


DATA_DIR = Path("/mnt/d/pumpapi-replay/derived/enriched")
SOL_PRICES = Path("/mnt/d/pumpapi-replay/derived/sol_prices.csv")
MT737_FEATURES = Path("/home/dev/workspace/data/results/mt737/entry_features.csv")
MT737_THRESHOLDS = Path("/home/dev/workspace/data/results/mt737/suggested_thresholds.csv")
DEFAULT_OUTPUT = Path("/home/dev/workspace/data/results/mt740")
START = date(2026, 4, 18)
END = date(2026, 5, 18)  # Exclusive: blind-period data is never read.
# One archive day is roughly 400 MB compressed; one-day workers stay beneath
# the memory ceiling while preserving the prior-day gate carry window.
CHUNK_DAYS = 1
OUTCOME_CHUNK_DAYS = 5
FIVE_MINUTES_MS = 300_000
USABLE_DELAY_TOLERANCE_MS = 30_000

# Exact complete CLI-only control configuration used by the V2 replay tests.
GATE = {
    "mcap_floor": 5_100.0,
    "mcap_ceiling": 50_000.0,
    "min_age_seconds": 0.0,
    "max_age_seconds": 1_320.0,
    "age_offset_seconds": 39.0,
    "txn_count_adjustment": 1.0,
    "min_volume_usd": 100.0,
    "min_volume_to_mcap_ratio": 0.005,
    "max_volume_to_mcap_ratio": 50.0,
    "min_buy_sell_ratio": 0.5,
    "min_pool_sol": 5.0,
    "creator_holdings_max": 0.0,
    "score_threshold_bonding": 40.0,
    "score_threshold_graduated": 40.0,
    "blocked_weekdays": (2,),
    "blocked_hours_utc": (0, 7),
}


def paths(start: date, end: date) -> list[Path]:
    result: list[Path] = []
    current = start
    while current < end:
        path = DATA_DIR / f"{current.isoformat()}.parquet"
        if not path.is_file():
            raise FileNotFoundError(path)
        result.append(path)
        current += timedelta(days=1)
    return result


def sql_paths(items: list[Path]) -> str:
    return ", ".join(repr(str(item)) for item in items)


def ms(value: date) -> int:
    return int(datetime.combine(value, datetime.min.time(), UTC).timestamp() * 1000)


def finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def fmt(value: float | None, digits: int = 4) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as source:
        return list(csv.DictReader(source))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(rows[0]) if rows else []
    with path.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def sol_price_values(start: date, end: date) -> str:
    prices: dict[str, float] = {}
    for row in read_csv(SOL_PRICES):
        price = finite(row.get("sol_usd"))
        if row.get("date") and price is not None:
            prices[row["date"][:10]] = price
    values = []
    current = start
    while current < end:
        price = prices.get(current.isoformat())
        if price is None:
            raise RuntimeError(f"No SOL price for {current}")
        values.append(f"(DATE '{current.isoformat()}', {price:.12f})")
        current += timedelta(days=1)
    return ", ".join(values)


def worker(chunk_start: date, chunk_end: date, output: Path, mode: str) -> None:
    """Write one bounded gate or outcome partition in a child process."""

    output.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    connection.execute("SET temp_directory='/tmp/mt740-duckdb'")
    connection.execute("SET threads=4")
    start_ms, end_ms = ms(chunk_start), ms(chunk_end)
    if mode == "gate":
        previous = max(START, chunk_start - timedelta(days=1))
        gate_paths = paths(previous, chunk_end)
        prices = sol_price_values(previous, chunk_end)
        gate_query = f"""
        COPY (
            WITH prices(day, sol_usd) AS (VALUES {prices}),
            age_limited AS (
                SELECT *
                FROM read_parquet([{sql_paths(gate_paths)}])
                WHERE seconds_since_birth BETWEEN 0 AND {GATE['max_age_seconds']}
            ), running AS (
                SELECT *,
                    sum(coalesce(buy_volume_sol, 0)) OVER w AS cumulative_buy_sol,
                    sum(coalesce(sell_volume_sol, 0)) OVER w AS cumulative_sell_sol,
                    sum(coalesce(trade_count, 0)) OVER w AS cumulative_trade_count
                FROM age_limited
                WINDOW w AS (PARTITION BY mint ORDER BY bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ), measured AS (
                SELECT running.*, prices.sol_usd,
                    seconds_since_birth + {GATE['age_offset_seconds']} AS corrected_age,
                    (cumulative_buy_sol + cumulative_sell_sol) * prices.sol_usd AS volume_usd,
                    CASE WHEN lower(coalesce(pool, '')) = 'pump' AND NOT coalesce(graduated_this_bar, false)
                         THEN 'bonding' ELSE 'graduated' END AS pool_type
                FROM running
                JOIN prices ON CAST(to_timestamp(bar_time / 1000.0) AS DATE) = prices.day
            ), scored AS (
                SELECT *,
                    volume_usd / nullif(market_cap_usd, 0) AS volume_mcap_ratio,
                    cumulative_buy_sol * sol_usd / nullif(cumulative_sell_sol * sol_usd, 0) AS buy_sell_ratio,
                    round(
                        least((cumulative_buy_sol * sol_usd / greatest(cumulative_sell_sol * sol_usd, 1.0)) / 2.0, 1.0) * 40.0
                        + least((volume_usd / nullif(market_cap_usd, 0)) / 0.05, 1.0) * 30.0
                        + least((cumulative_trade_count * {GATE['txn_count_adjustment']}) / (4.0 * CASE
                            WHEN corrected_age < 60 THEN 3 WHEN corrected_age < 180 THEN 5
                            WHEN corrected_age < 300 THEN 8 WHEN corrected_age < 600 THEN 12 ELSE 16 END), 1.0) * 15.0
                        + least(volume_usd / (10.0 * {GATE['min_volume_usd']}), 1.0) * 15.0,
                        1
                    ) AS strength_score
                FROM measured
            )
            SELECT CAST(mint AS VARCHAR) AS mint, CAST(bar_time AS BIGINT) AS gate_time
            FROM scored
            WHERE bar_time >= {start_ms} AND bar_time < {end_ms}
              AND seconds_since_birth >= {GATE['min_age_seconds']}
              AND corrected_age <= {GATE['max_age_seconds']}
              AND market_cap_usd BETWEEN {GATE['mcap_floor']} AND {GATE['mcap_ceiling']}
              AND max_sol_in_pool >= {GATE['min_pool_sol']}
              AND close > 0
              AND volume_usd >= {GATE['min_volume_usd']}
              AND volume_mcap_ratio BETWEEN {GATE['min_volume_to_mcap_ratio']} AND {GATE['max_volume_to_mcap_ratio']}
              AND buy_sell_ratio >= {GATE['min_buy_sell_ratio']}
              AND cumulative_trade_count * {GATE['txn_count_adjustment']} >= CASE
                    WHEN corrected_age < 60 THEN 3 WHEN corrected_age < 180 THEN 5
                    WHEN corrected_age < 300 THEN 8 WHEN corrected_age < 600 THEN 12 ELSE 16 END
              AND (creator_holdings_pct IS NULL OR creator_holdings_pct <= {GATE['creator_holdings_max']})
              AND strength_score >= CASE WHEN pool_type = 'bonding'
                                        THEN {GATE['score_threshold_bonding']}
                                        ELSE {GATE['score_threshold_graduated']} END
              AND date_part('isodow', to_timestamp(bar_time / 1000.0)) - 1 NOT IN (2)
              AND date_part('hour', to_timestamp(bar_time / 1000.0)) NOT IN (0, 7)
        ) TO '{str(output / 'gate_bars.csv')}' (HEADER, DELIMITER ',')
        """
        connection.execute(gate_query)
        connection.close()
        return

    # First graduation is deliberately global, matching MT-737 rather than
    # assuming a mint cannot carry a duplicate graduation marker across files.
    outcome_paths = paths(chunk_start, END)
    outcome_query = f"""
        COPY (
            WITH graduates AS (
                SELECT CAST(mint AS VARCHAR) AS mint, min(bar_time) AS graduation_time
                FROM read_parquet([{sql_paths(paths(START, END))}])
                WHERE graduated_this_bar
                GROUP BY mint
            ), entries AS (
                SELECT * FROM graduates
                WHERE graduation_time >= {start_ms} AND graduation_time < {end_ms}
            ), feature_rows AS (
                SELECT CAST(mint AS VARCHAR) AS mint,
                       CAST(entry_time AS BIGINT) AS entry_time,
                       CAST(entry_close AS DOUBLE) AS entry_close,
                       CAST(runner AS BOOLEAN) AS runner,
                       CAST(dud AS BOOLEAN) AS dud,
                       CAST(return_2m AS DOUBLE) AS return_2m,
                       CAST(return_5m AS DOUBLE) AS return_5m,
                       CAST(return_acceleration_1m_to_5m AS DOUBLE) AS return_acceleration_1m_to_5m,
                       CAST(max_pool_delta_5m AS DOUBLE) AS max_pool_delta_5m
                FROM read_csv_auto('{str(MT737_FEATURES)}', header=true)
            ), joined AS (
                SELECT entries.mint, entries.graduation_time, bars.bar_time, bars.close,
                       features.entry_close, features.runner, features.dud,
                       features.return_2m, features.return_5m,
                       features.return_acceleration_1m_to_5m, features.max_pool_delta_5m
                FROM entries
                LEFT JOIN feature_rows AS features
                  ON entries.mint = features.mint AND entries.graduation_time = features.entry_time
                LEFT JOIN read_parquet([{sql_paths(outcome_paths)}]) AS bars
                  ON entries.mint = CAST(bars.mint AS VARCHAR) AND bars.bar_time >= entries.graduation_time
            ), minute_bars AS (
                SELECT mint, graduation_time,
                       min(bar_time) FILTER (WHERE bar_time >= graduation_time + {FIVE_MINUTES_MS}
                                               AND bar_time <= graduation_time + {FIVE_MINUTES_MS + USABLE_DELAY_TOLERANCE_MS}) AS minute_five_time
                FROM joined
                GROUP BY mint, graduation_time
            ), outcomes AS (
                SELECT joined.mint, joined.graduation_time, joined.entry_close, joined.runner, joined.dud,
                       joined.return_2m, joined.return_5m, joined.return_acceleration_1m_to_5m,
                       joined.max_pool_delta_5m, minute_bars.minute_five_time,
                       max(joined.close) AS peak_from_graduation,
                       arg_min(joined.close, joined.bar_time) FILTER (WHERE joined.bar_time = minute_bars.minute_five_time) AS minute_five_close,
                       max(joined.close) FILTER (WHERE joined.bar_time >= minute_bars.minute_five_time) AS peak_from_minute_five
                FROM joined
                JOIN minute_bars USING (mint, graduation_time)
                GROUP BY ALL
            )
            SELECT * FROM outcomes
        ) TO '{str(output / 'outcomes.csv')}' (HEADER, DELIMITER ',')
    """
    connection.execute(outcome_query)
    connection.close()


def run_workers(output: Path) -> None:
    gate_chunks = output / "gate_chunks"
    gate_chunks.mkdir(parents=True, exist_ok=True)
    current = START
    while current < END:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), END)
        part = gate_chunks / f"{current}_{chunk_end}"
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker", "--mode", "gate", "--start", current.isoformat(), "--end", chunk_end.isoformat(), "--output", str(part)],
            check=True,
        )
        current = chunk_end
    outcome_chunks = output / "outcome_chunks"
    outcome_chunks.mkdir(parents=True, exist_ok=True)
    current = START
    while current < END:
        chunk_end = min(current + timedelta(days=OUTCOME_CHUNK_DAYS), END)
        part = outcome_chunks / f"{current}_{chunk_end}"
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker", "--mode", "outcome", "--start", current.isoformat(), "--end", chunk_end.isoformat(), "--output", str(part)],
            check=True,
        )
        current = chunk_end


def fraction(count: int, total: int) -> float:
    return count / total if total else 0.0


def numeric(row: dict[str, str], name: str) -> float | None:
    return finite(row.get(name))


def pnl(row: dict[str, str], peak: str, entry: str) -> float | None:
    top, base = numeric(row, peak), numeric(row, entry)
    if top is None or base is None or base <= 0:
        return None
    return top / base - 1.0


def stats(values: list[float]) -> dict[str, float | int | None]:
    return {"n": len(values), "median": median(values) if values else None, "mean": mean(values) if values else None}


def load_thresholds() -> dict[str, float]:
    return {row["feature"]: float(row["threshold"]) for row in read_csv(MT737_THRESHOLDS)}


def render_report(output: Path) -> None:
    chunks = sorted((output / "gate_chunks").glob("*/gate_bars.csv"))
    gate_rows = [row for part in chunks for row in read_csv(part)]
    outcomes = [row for part in sorted((output / "outcome_chunks").glob("*/outcomes.csv")) for row in read_csv(part)]
    graduates = {row["mint"]: int(row["graduation_time"]) for row in outcomes}
    # Outcomes contain all graduates, including the 609 MT-737-incomplete rows.
    gate_on_graduate = [row for row in gate_rows if row["mint"] in graduates]
    first_gate: dict[str, int] = {}
    for row in gate_on_graduate:
        first_gate[row["mint"]] = min(first_gate.get(row["mint"], int(row["gate_time"])), int(row["gate_time"]))
    timing = [first_gate[mint] - graduates[mint] for mint in first_gate]
    before, at, after = (
        sum(item < 0 for item in timing),
        sum(item == 0 for item in timing),
        sum(item > 0 for item in timing),
    )
    delay_usable = [row for row in outcomes if row.get("minute_five_time") not in (None, "") and numeric(row, "minute_five_close")]
    feature_complete = [row for row in delay_usable if numeric(row, "entry_close") is not None]
    delayed_pnl = {id(row): pnl(row, "peak_from_minute_five", "minute_five_close") for row in delay_usable}
    graduation_pnl = {id(row): pnl(row, "peak_from_graduation", "entry_close") for row in feature_complete}

    groups = {
        "all feature-complete": feature_complete,
        "runners": [row for row in feature_complete if row.get("runner", "").lower() == "true"],
        "duds": [row for row in feature_complete if row.get("dud", "").lower() == "true"],
    }
    pnl_rows: list[dict[str, Any]] = []
    for label, rows in groups.items():
        grad_values = [graduation_pnl[id(row)] for row in rows if graduation_pnl[id(row)] is not None]
        delay_values = [delayed_pnl[id(row)] for row in rows if delayed_pnl[id(row)] is not None]
        pnl_rows.append({"group": label, **{f"graduation_{key}": value for key, value in stats(grad_values).items()}, **{f"minute_five_{key}": value for key, value in stats(delay_values).items()}})
    runner_rows = groups["runners"]
    retained_upside = sum(max(delayed_pnl[id(row)] or 0.0, 0.0) for row in runner_rows) / sum(max(graduation_pnl[id(row)] or 0.0, 0.0) for row in runner_rows) if runner_rows else 0.0

    thresholds = load_thresholds()
    features = ["return_5m", "max_pool_delta_5m", "return_2m", "return_acceleration_1m_to_5m"]
    base = feature_complete
    filter_rows: list[dict[str, Any]] = []
    filters: list[tuple[str, list[str]]] = [(name, [name]) for name in features] + [("combined_and", features)]
    for label, rules in filters:
        passing = [row for row in base if all((numeric(row, rule) is not None and numeric(row, rule) >= thresholds[rule]) for rule in rules)]
        values = [delayed_pnl[id(row)] for row in passing if delayed_pnl[id(row)] is not None]
        filter_rows.append({
            "filter": label,
            "pass_n": len(passing),
            "retention_pct": fraction(len(passing), len(base)) * 100,
            "runner_rate_pct": fraction(sum(row.get("runner", "").lower() == "true" for row in passing), len(passing)) * 100,
            "dud_rate_pct": fraction(sum(row.get("dud", "").lower() == "true" for row in passing), len(passing)) * 100,
            "median_max_pnl_pct": median(values) * 100 if values else None,
            "mean_max_pnl_pct": mean(values) * 100 if values else None,
        })
    base_values = [delayed_pnl[id(row)] for row in base if delayed_pnl[id(row)] is not None]
    filter_rows.insert(0, {
        "filter": "unfiltered_base",
        "pass_n": len(base), "retention_pct": 100.0,
        "runner_rate_pct": fraction(sum(row.get("runner", "").lower() == "true" for row in base), len(base)) * 100,
        "dud_rate_pct": fraction(sum(row.get("dud", "").lower() == "true" for row in base), len(base)) * 100,
        "median_max_pnl_pct": median(base_values) * 100 if base_values else None,
        "mean_max_pnl_pct": mean(base_values) * 100 if base_values else None,
    })
    write_csv(output / "pnl_comparison.csv", pnl_rows)
    write_csv(output / "filter_results.csv", filter_rows)

    gate_mints = {row["mint"] for row in gate_rows}
    report = [
        "# MT-740 Feature Tradeability", "", "## Scope", "",
        f"Training archive only: {START} through {END} exclusive. No May 18 or later Parquet was read.",
        "The gate is mirrored from `capacity_sweep_bt_v2.py`'s per-bar candidate checks with the fixed control configuration (including cumulative score inputs, weekday/hour blocks, pool/mcap/age constraints, and creator holdings). This is the candidate gate, not the capacity/loss-ban execution state.",
        "A usable minute-five mark is the first close at 300-330 seconds after graduation. This tolerance matches the replay's stale-entry tolerance; a later isolated mark is not treated as a five-minute price.", "",
        "## Gate Versus Graduation", "",
        f"- Gate-passing bars: **{len(gate_rows):,}** across **{len(gate_mints):,}** mints.",
        f"- MT-737 first graduates: **{len(graduates):,}** mints.",
        f"- Gate bars on a graduate mint: **{len(gate_on_graduate):,}** ({fraction(len(gate_on_graduate), len(gate_rows)) * 100:.1f}%); gate mints that ever graduate: **{len(set(gate_mints) & set(graduates)):,}** ({fraction(len(set(gate_mints) & set(graduates)), len(gate_mints)) * 100:.1f}%).",
        f"- Gate bars on never-graduating mints: **{len(gate_rows) - len(gate_on_graduate):,}** ({fraction(len(gate_rows) - len(gate_on_graduate), len(gate_rows)) * 100:.1f}%). Those bars have no MT-737 post-graduation feature definition.",
        f"- First gate fire on graduate mints: **{len(timing):,}**. Before graduation: **{before:,}** ({fraction(before, len(timing)) * 100:.1f}%); same bar: **{at:,}** ({fraction(at, len(timing)) * 100:.1f}%); after: **{after:,}** ({fraction(after, len(timing)) * 100:.1f}%). Median first-fire offset: **{fmt(median(timing) / 1000 if timing else None, 1)} s**; mean: **{fmt(mean(timing) / 1000 if timing else None, 1)} s**.", "",
        "## Five-Minute Delay", "",
        f"- Usable minute-five mark: **{len(delay_usable):,}/{len(outcomes):,}** graduates. Missing/unusable: **{len(outcomes) - len(delay_usable):,}** ({fraction(len(outcomes) - len(delay_usable), len(outcomes)) * 100:.1f}%), from a dead/rugged path, archive gap, or no close in the 300-330 s window.",
        f"- PnL comparison below holds the cohort fixed to the {len(feature_complete):,} feature-complete graduates that also have a usable minute-five mark. Runner/dud labels remain MT-737's original, liquidity-screened graduation labels; they are not re-tuned or relabeled.", "",
        "| group | n | graduation median | graduation mean | minute-five median | minute-five mean |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        *[f"| {row['group']} | {row['graduation_n']} | {fmt(row['graduation_median'] * 100 if row['graduation_median'] is not None else None, 2)}% | {fmt(row['graduation_mean'] * 100 if row['graduation_mean'] is not None else None, 2)}% | {fmt(row['minute_five_median'] * 100 if row['minute_five_median'] is not None else None, 2)}% | {fmt(row['minute_five_mean'] * 100 if row['minute_five_mean'] is not None else None, 2)}% |" for row in pnl_rows],
        "- Means are reported uncapped as requested. Extreme archived close marks dominate them, so the medians and the retained-upside ratio are the interpretable delay comparison; no outlier cap was introduced.",
        f"- Positive runner upside retained after the delay: **{retained_upside * 100:.1f}%** (sum of delayed positive max-PnL / sum of graduation positive max-PnL).", "",
        "## Fixed MT-737 Thresholds at Minute Five", "",
        "All threshold directions are `>=`, exactly as MT-737 wrote them. The base is feature-complete graduates with a usable minute-five mark; PnL is max PnL from that delayed mark. The combined rule is a strict AND, not a new tuning pass.", "",
        "| filter | pass n | retained | runner rate | dud rate | median max PnL | mean max PnL |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        *[f"| {row['filter']} | {row['pass_n']:,} | {row['retention_pct']:.1f}% | {row['runner_rate_pct']:.1f}% | {row['dud_rate_pct']:.1f}% | {fmt(row['median_max_pnl_pct'], 2)}% | {fmt(row['mean_max_pnl_pct'], 2)}% |" for row in filter_rows],
        "", "## Verdict", "",
        ("**No.** The actual per-bar strategy gate materially trades mints and timing states outside MT-737's graduate-at-minute-zero population, and every never-graduate gate bar lacks all four feature definitions. A usable strategy test must rebuild the features around gate candidates, using only information available at the intended delayed entry, then perform the already-reserved fixed blind-period test. The graduation-cohort delay and threshold figures above are descriptive evidence only, not a deployable gate."),
        "", "## Files", "", "- `filter_results.csv`: fixed-threshold delayed-entry cohort metrics.", "- `pnl_comparison.csv`: graduation versus minute-five PnL summary.", "- `gate_chunks/*/gate_bars.csv`: gate-passing bar identifiers.", "- `outcome_chunks/*/outcomes.csv`: first-graduate delayed mark outcomes and MT-737 features.",
    ]
    (output / "MT740_REPORT.md").write_text("\n".join(report) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--mode", choices=("gate", "outcome"), default="gate")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()
    if args.worker:
        if args.start is None or args.end is None:
            parser.error("--worker requires --start and --end")
        worker(args.start, args.end, args.output, args.mode)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    run_workers(args.output)
    render_report(args.output)


if __name__ == "__main__":
    main()
