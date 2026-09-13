#!/usr/bin/env python3
"""MT-765: bounded one-month candidate-feature prove-out.

This detached analysis reads only 2026-04-18 through 2026-05-18.  Every
archive scan is one UTC day in a run-capped subprocess; no Hive writes, table
creation, engine changes, or PumpApi service interaction occur.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import resource
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
LABELS = Path("/home/dev/workspace/data/results/mt739/label_chunks")
ROOT = Path("/workspace/shared/MT-765")
START = date(2026, 4, 18)
END = date(2026, 5, 19)  # Exclusive. Never read this day or a later one.
SAMPLE_DAYS = (date(2026, 4, 18), date(2026, 4, 19), date(2026, 4, 20))
MEMORY_LIMIT = "4GB"
RUNTIME_CAP_S = 3 * 60 * 60
MARK_TOLERANCE_MS = 30_000

BASELINE_COLUMNS = (
    "price",
    "market_cap_usd",
    "pool_sol",
    "trade_count_1m",
    "buy_volume_1m",
    "sell_volume_1m",
    "return_1m",
    "return_2m",
    "return_5m",
)
CANDIDATE_COLUMNS = (
    "pool_to_market_cap",
    "pool_growth_1m",
    "pool_drawdown",
    "buy_sell_volume_ratio",
    "net_flow_1m",
    "distance_below_running_high",
    "new_high_count",
    "longest_gap_no_trades_s",
    "volatility_1m",
    "time_since_last_trade_s",
    "launches_same_minute",
    "graduations_same_hour",
    "hour_of_day_utc",
    "day_of_week_utc",
    "age_at_checkpoint_s",
    "graduated",
    "seconds_since_graduation",
    *BASELINE_COLUMNS,
)
OUTCOMES = ("out_peak_multiple", "out_close_5m", "out_close_20m")


def days() -> list[date]:
    result: list[date] = []
    current = START
    while current < END:
        result.append(current)
        current += timedelta(days=1)
    return result


def path_for(day: date) -> Path:
    path = ARCHIVE / f"{day.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def label_path(day: date) -> Path:
    path = LABELS / f"{day.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def quote(value: Path | str) -> str:
    return repr(str(value))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def sci(value: Any) -> str:
    number = finite(value)
    return "" if number is None else f"{number:.8e}"


def duck(temp_name: str) -> duckdb.DuckDBPyConnection:
    temp = ROOT / "duckdb_tmp" / temp_name
    temp.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    connection.execute("SET threads = 4")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(f"SET temp_directory = {quote(temp)}")
    return connection


def schema_columns() -> list[str]:
    with duck("phase0-schema") as connection:
        return [row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM read_parquet({quote(path_for(START))})").fetchall()]


def phase0() -> None:
    """Write source-schema coverage and an explicit no-proxy feasibility table."""
    schema_path = path_for(START)
    with duck("phase0") as connection:
        schema = connection.execute(f"DESCRIBE SELECT * FROM read_parquet({quote(schema_path)})").fetchall()
        sources = ", ".join(quote(path_for(day)) for day in SAMPLE_DAYS)
    # DuckDB UNPIVOT produces a common type, so calculate typed coverage directly.
    coverage_rows: list[dict[str, Any]] = []
    with duck("phase0-coverage") as connection:
        for name, dtype, nullable, *_ in schema:
            non_null, distinct = connection.execute(
                f"SELECT count({name}), count(DISTINCT {name}) FROM read_parquet([{sources}])"
            ).fetchone()
            coverage_rows.append({
                "record_type": "field_coverage_3day",
                "candidate": "",
                "status": "dead" if non_null and distinct <= 1 else "present",
                "required_source_fields": name,
                "reason": "constant over non-null sample" if non_null and distinct <= 1 else "",
                "field": name,
                "dtype": dtype,
                "non_null_count": non_null,
                "distinct_value_count": distinct,
            })

    present = {row["field"]: row for row in coverage_rows}
    def status_for(fields: tuple[str, ...], reason: str = "") -> tuple[str, str]:
        missing = [field for field in fields if field not in present]
        if missing:
            return "blocked", f"missing source field(s): {', '.join(missing)}"
        dead = [field for field in fields if present[field]["status"] == "dead"]
        if dead:
            return "dead", f"constant source field(s): {', '.join(dead)}"
        return "computable", reason

    candidates: list[tuple[str, tuple[str, ...], str]] = [
        ("pool_to_market_cap", ("min_sol_in_pool", "market_cap_usd"), "current pool / current market cap"),
        ("pool_growth_1m", ("min_sol_in_pool", "bar_time"), "current pool versus last pool at or before 1m"),
        ("pool_drawdown", ("min_sol_in_pool", "bar_time"), "current pool / running pool maximum - 1"),
        ("buy_sell_volume_ratio", ("buy_volume_sol", "sell_volume_sol"), "trailing one-minute aggregate volumes"),
        ("net_flow_1m", ("buy_volume_sol", "sell_volume_sol"), "trailing one-minute buy minus sell volume"),
        ("buy_count_vs_sell_count", (), "blocked: bars expose aggregate trade_count only, not side counts"),
        ("largest_single_trade", (), "blocked: no per-trade amount detail"),
        ("largest_trade_share", (), "blocked: no per-trade amount detail"),
        ("average_trade_size", (), "blocked: aggregate volume/trade count supports an arithmetic proxy only; proxies are prohibited"),
        ("median_trade_size", (), "blocked: no per-trade amount detail"),
        ("top5_trade_volume_share", (), "blocked: no per-trade amount detail"),
        ("unique_buyers", (), "blocked: no buyer identity or buyer-side wallet counts"),
        ("unique_sellers", (), "blocked: no seller identity or seller-side wallet counts"),
        ("buyer_to_seller_ratio", (), "blocked: no buyer/seller counts"),
        ("new_wallets_per_minute", (), "blocked: no wallet identities or per-bar new-wallet count"),
        ("repeat_buyers", (), "blocked: no buyer identities"),
        ("distance_below_running_high", ("close", "bar_time"), "current close / running close maximum - 1"),
        ("new_high_count", ("high", "bar_time"), "strict running-high events"),
        ("longest_gap_no_trades_s", ("trade_count", "bar_time"), "gaps between positive-trade bars"),
        ("volatility_1m", ("close", "bar_time"), "one-minute close standard deviation / mean close"),
        ("time_since_last_trade_s", ("trade_count", "bar_time"), "checkpoint minus latest positive-trade bar"),
        ("launches_same_minute", ("mint", "bar_time"), "first observed bar minute per coin"),
        ("graduations_same_hour", ("graduated_this_bar", "bar_time"), "graduation markers per UTC hour"),
        ("hour_of_day_utc", ("bar_time",), "checkpoint UTC hour"),
        ("day_of_week_utc", ("bar_time",), "checkpoint UTC day of week"),
        ("age_at_checkpoint_s", ("bar_time",), "checkpoint minus first observed bar"),
        ("graduated", ("graduated_this_bar",), "graduation marker observed by checkpoint"),
        ("seconds_since_graduation", ("graduated_this_bar", "bar_time"), "checkpoint minus first graduation marker"),
        ("end_state", (), "blocked: it requires future bars or future rug labels and would leak beyond the checkpoint"),
        ("price", ("close",), "checkpoint close"),
        ("market_cap_usd", ("market_cap_usd",), "checkpoint market cap"),
        ("pool_sol", ("min_sol_in_pool",), "checkpoint pool depth"),
        ("trade_count_1m", ("trade_count", "bar_time"), "trailing one-minute aggregate trade count"),
        ("buy_volume_1m", ("buy_volume_sol", "bar_time"), "trailing one-minute buy volume"),
        ("sell_volume_1m", ("sell_volume_sol", "bar_time"), "trailing one-minute sell volume"),
        ("return_1m", ("close", "bar_time"), "checkpoint close versus close one minute earlier"),
        ("return_2m", ("close", "bar_time"), "checkpoint close versus close two minutes earlier"),
        ("return_5m", ("close", "bar_time"), "checkpoint close versus close five minutes earlier"),
    ]
    feasibility: list[dict[str, Any]] = []
    for candidate, fields, note in candidates:
        if note.startswith("blocked:"):
            status, reason = "blocked", note.removeprefix("blocked: ")
        else:
            status, reason = status_for(fields, note)
        feasibility.append({
            "record_type": "candidate_feasibility",
            "candidate": candidate,
            "status": status,
            "required_source_fields": ", ".join(fields),
            "reason": reason,
            "field": "",
            "dtype": "",
            "non_null_count": "",
            "distinct_value_count": "",
        })
    write_csv(ROOT / "phase0_feasibility.csv", [*feasibility, *coverage_rows], list(feasibility[0]))
    (ROOT / "schema_2026-04-18.txt").write_text(
        "\n".join(f"{name}\t{dtype}\tnullable={nullable}" for name, dtype, nullable, *_ in schema) + "\n",
        encoding="utf-8",
    )


def day_summary(day: date) -> None:
    destination = ROOT / "summary_chunks" / f"{day.isoformat()}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with duck(f"summary-{day.isoformat()}") as connection:
        connection.execute(
            f"""
            COPY (
              SELECT mint, min(bar_time) AS first_bar_time, max(bar_time) AS last_bar_time,
                     max(min_sol_in_pool) AS max_pool_sol, sum(coalesce(trade_count, 0)) AS total_trades,
                     bool_or(coalesce(graduated_this_bar, false)) AS graduated,
                     count(*) FILTER (WHERE mint IS NULL OR trim(mint) = '' OR bar_time IS NULL OR close IS NULL OR close <= 0) AS bad_bar_rows
              FROM read_parquet({quote(path_for(day))})
              WHERE mint IS NOT NULL AND trim(mint) <> '' AND bar_time IS NOT NULL
              GROUP BY mint
            ) TO {quote(destination)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    worker_stats(day, "summary")


def worker_stats(day: date, stage: str) -> None:
    stats = ROOT / "worker_stats"
    stats.mkdir(parents=True, exist_ok=True)
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    (stats / f"{stage}-{day.isoformat()}.json").write_text(
        json.dumps({"day": day.isoformat(), "stage": stage, "peak_rss_kb": rss_kb}) + "\n",
        encoding="utf-8",
    )


def aggregate_population() -> tuple[list[int], list[str]]:
    chunks = ", ".join(quote(ROOT / "summary_chunks" / f"{day.isoformat()}.parquet") for day in days())
    output = ROOT / "population.parquet"
    with duck("population") as connection:
        connection.execute(
            f"""
            COPY (
              SELECT mint, min(first_bar_time) AS first_bar_time, max(last_bar_time) AS last_bar_time,
                     max(max_pool_sol) AS max_pool_sol, sum(total_trades) AS total_trades,
                     bool_or(graduated) AS graduated, sum(bad_bar_rows) AS bad_bar_rows
              FROM read_parquet([{chunks}]) GROUP BY mint
            ) TO {quote(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        # Store first day without relying on host timezone conversion.
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE population AS SELECT * FROM read_parquet({quote(output)});
            COPY (
              SELECT *, CAST(to_timestamp(first_bar_time / 1000.0) AT TIME ZONE 'UTC' AS DATE) AS first_day,
                     (last_bar_time - first_bar_time) / 1000.0 AS lifespan_s,
                     max_pool_sol >= 1 AND total_trades >= 10 AS working
              FROM population
            ) TO {quote(ROOT / 'population_with_day.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        lifespans = [row[0] for row in connection.execute(
            f"SELECT lifespan_s FROM read_parquet({quote(ROOT / 'population_with_day.parquet')}) WHERE working"
        ).fetchall()]
    lifespans.sort()
    checkpoints: list[int] = []
    for survival in (0.90, 0.75, 0.50, 0.25, 0.10):
        if not lifespans:
            checkpoints.append(0)
            continue
        index = max(0, min(len(lifespans) - 1, math.ceil((1 - survival) * len(lifespans)) - 1))
        checkpoints.append(min(3600, int(math.floor(lifespans[index]))))
    return checkpoints, []


def phase1_phase2(checkpoints: list[int]) -> None:
    source = quote(ROOT / "population_with_day.parquet")
    rows: list[dict[str, Any]] = []
    with duck("phase1-phase2") as connection:
        for target, checkpoint in zip(("90%", "75%", "50%", "25%", "10%"), checkpoints, strict=True):
            total, graduates = connection.execute(
                f"SELECT count(*), count(*) FILTER (WHERE graduated) FROM read_parquet({source}) WHERE working AND lifespan_s >= {checkpoint}"
            ).fetchone()
            rows.append({"record_type": "survival_checkpoint", "checkpoint_seconds": checkpoint, "survival_target": target, "alive_count": total, "graduated_alive_count": graduates, "not_graduated_alive_count": total - graduates, "value": "", "p25": "", "p50": "", "p75": "", "p90": "", "pool_floor": "", "trade_floor": "", "survivor_count": ""})
        all_count, graduated = connection.execute(f"SELECT count(*), count(*) FILTER (WHERE graduated) FROM read_parquet({source})").fetchone()
        rows.extend([
            {"record_type": "population_count", "checkpoint_seconds": "", "survival_target": "", "alive_count": "", "graduated_alive_count": "", "not_graduated_alive_count": "", "value": all_count, "p25": "", "p50": "", "p75": "", "p90": "", "pool_floor": "", "trade_floor": "", "survivor_count": ""},
            {"record_type": "graduated_count", "checkpoint_seconds": "", "survival_target": "", "alive_count": "", "graduated_alive_count": "", "not_graduated_alive_count": "", "value": graduated, "p25": "", "p50": "", "p75": "", "p90": "", "pool_floor": "", "trade_floor": "", "survivor_count": ""},
            {"record_type": "not_graduated_count", "checkpoint_seconds": "", "survival_target": "", "alive_count": "", "graduated_alive_count": "", "not_graduated_alive_count": "", "value": all_count - graduated, "p25": "", "p50": "", "p75": "", "p90": "", "pool_floor": "", "trade_floor": "", "survivor_count": ""},
        ])
        for group, predicate in (("graduated", "graduated"), ("not_graduated", "NOT graduated")):
            for metric in ("max_pool_sol", "total_trades", "lifespan_s"):
                count, p25, p50, p75, p90 = connection.execute(
                    f"SELECT count({metric}), quantile_cont({metric}, .25), quantile_cont({metric}, .5), quantile_cont({metric}, .75), quantile_cont({metric}, .9) FROM read_parquet({source}) WHERE {predicate}"
                ).fetchone()
                rows.append({"record_type": f"distribution_{group}", "checkpoint_seconds": "", "survival_target": metric, "alive_count": "", "graduated_alive_count": "", "not_graduated_alive_count": "", "value": count, "p25": sci(p25), "p50": sci(p50), "p75": sci(p75), "p90": sci(p90), "pool_floor": "", "trade_floor": "", "survivor_count": ""})
        for pool in (1, 5, 10, 25, 50, 100):
            for trades in (10, 50, 200):
                count = connection.execute(f"SELECT count(*) FROM read_parquet({source}) WHERE max_pool_sol >= {pool} AND total_trades >= {trades}").fetchone()[0]
                rows.append({"record_type": "floor_grid", "checkpoint_seconds": "", "survival_target": "", "alive_count": "", "graduated_alive_count": "", "not_graduated_alive_count": "", "value": "", "p25": "", "p50": "", "p75": "", "p90": "", "pool_floor": pool, "trade_floor": trades, "survivor_count": count})
    phase1 = [row for row in rows if row["record_type"] == "survival_checkpoint"]
    write_csv(ROOT / "phase1_survival.csv", phase1, list(rows[0]))
    write_csv(ROOT / "phase2_population.csv", rows, list(rows[0]))


def feature_day(day: date, checkpoints: list[int]) -> None:
    """Snapshot first-observed-on-day working coins. Forward reads stay in this day file."""
    destination = ROOT / "feature_chunks" / f"{day.isoformat()}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_values = ", ".join(f"({value})" for value in checkpoints)
    day_end = int(datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC).timestamp() * 1000)
    with duck(f"features-{day.isoformat()}") as connection:
        connection.execute(
            f"""
            COPY (
              WITH population AS (
                SELECT mint, first_bar_time, last_bar_time, graduated AS ever_graduated,
                       first_day, lifespan_s, working
                FROM read_parquet({quote(ROOT / 'population_with_day.parquet')})
                WHERE working AND first_day = DATE '{day.isoformat()}'
              ), bars AS (
                SELECT b.* FROM read_parquet({quote(path_for(day))}) b JOIN population p USING (mint)
                WHERE b.close > 0
              ), launches AS (
                SELECT date_trunc('minute', to_timestamp(first_bar_time / 1000.0)) AS minute, count(*) AS launches_same_minute
                FROM (SELECT mint, min(bar_time) AS first_bar_time FROM bars GROUP BY mint)
                GROUP BY 1
              ), graduations AS (
                SELECT date_trunc('hour', to_timestamp(bar_time / 1000.0)) AS hour, count(*) AS graduations_same_hour
                FROM bars WHERE graduated_this_bar GROUP BY 1
              ), targets AS (
                SELECT p.*, c.checkpoint_seconds::INTEGER AS checkpoint_seconds,
                       p.first_bar_time + c.checkpoint_seconds * 1000 AS target_time
                FROM population p CROSS JOIN (VALUES {checkpoint_values}) c(checkpoint_seconds)
                WHERE p.lifespan_s >= c.checkpoint_seconds
              ), marks AS (
                SELECT t.*, min(b.bar_time) AS checkpoint_time
                FROM targets t JOIN bars b ON b.mint = t.mint
                  AND b.bar_time >= t.target_time AND b.bar_time <= t.target_time + {MARK_TOLERANCE_MS}
                GROUP BY ALL
              ), history AS (
                SELECT m.*, b.bar_time, b.close, b.high, b.min_sol_in_pool, b.market_cap_usd,
                       b.buy_volume_sol, b.sell_volume_sol, b.trade_count, b.graduated_this_bar,
                       max(b.close) OVER (PARTITION BY m.mint, m.checkpoint_seconds ORDER BY b.bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_high,
                       max(b.high) OVER (PARTITION BY m.mint, m.checkpoint_seconds ORDER BY b.bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_high
                FROM marks m JOIN bars b ON b.mint = m.mint AND b.bar_time <= m.checkpoint_time
              ), features AS (
                SELECT mint, checkpoint_seconds, checkpoint_time, first_bar_time, last_bar_time, ever_graduated,
                       arg_max(close, bar_time) AS price, arg_max(market_cap_usd, bar_time) AS market_cap_usd,
                       arg_max(min_sol_in_pool, bar_time) AS pool_sol,
                       arg_max(min_sol_in_pool, bar_time) / nullif(arg_max(market_cap_usd, bar_time), 0) AS pool_to_market_cap,
                       arg_max(min_sol_in_pool, bar_time) / nullif(arg_max(min_sol_in_pool, bar_time) FILTER (WHERE bar_time <= checkpoint_time - 60000), 0) - 1 AS pool_growth_1m,
                       arg_max(min_sol_in_pool, bar_time) / nullif(max(min_sol_in_pool), 0) - 1 AS pool_drawdown,
                       sum(trade_count) FILTER (WHERE bar_time > checkpoint_time - 60000) AS trade_count_1m,
                       sum(buy_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS buy_volume_1m,
                       sum(sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS sell_volume_1m,
                       sum(buy_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) / nullif(sum(sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000), 0) AS buy_sell_volume_ratio,
                       sum(buy_volume_sol - sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS net_flow_1m,
                       arg_max(close, bar_time) / nullif(max(close), 0) - 1 AS distance_below_running_high,
                       count(*) FILTER (WHERE high > coalesce(prior_high, -1)) AS new_high_count,
                       stddev_samp(close) FILTER (WHERE bar_time > checkpoint_time - 60000) / nullif(avg(close) FILTER (WHERE bar_time > checkpoint_time - 60000), 0) AS volatility_1m,
                       (checkpoint_time - max(bar_time) FILTER (WHERE trade_count > 0)) / 1000.0 AS time_since_last_trade_s,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time) FILTER (WHERE bar_time <= checkpoint_time - 60000), 0) - 1 AS return_1m,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time) FILTER (WHERE bar_time <= checkpoint_time - 120000), 0) - 1 AS return_2m,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time) FILTER (WHERE bar_time <= checkpoint_time - 300000), 0) - 1 AS return_5m,
                       min(bar_time) FILTER (WHERE graduated_this_bar) AS graduation_time
                FROM history GROUP BY mint, checkpoint_seconds, checkpoint_time, first_bar_time, last_bar_time, ever_graduated
              ), gaps AS (
                SELECT mint, checkpoint_seconds, max((bar_time - prior_trade_time) / 1000.0) AS longest_gap_no_trades_s
                FROM (
                  SELECT h.mint, h.checkpoint_seconds, h.bar_time,
                         lag(h.bar_time) OVER (PARTITION BY h.mint, h.checkpoint_seconds ORDER BY h.bar_time) AS prior_trade_time
                  FROM history h WHERE h.trade_count > 0
                ) GROUP BY mint, checkpoint_seconds
              ), outcomes AS (
                SELECT f.mint, f.checkpoint_seconds,
                       max(b.close) / nullif(f.price, 0) AS out_peak_multiple,
                       arg_min(b.close, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 300000 AND b.bar_time <= f.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}) / nullif(f.price, 0) AS out_close_5m,
                       arg_min(b.close, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 1200000 AND b.bar_time <= f.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}) / nullif(f.price, 0) AS out_close_20m,
                       arg_min(b.min_sol_in_pool, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 300000 AND b.bar_time <= f.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}) AS out_pool_at_5m,
                       arg_min(b.min_sol_in_pool, b.bar_time) FILTER (WHERE b.bar_time >= f.checkpoint_time + 1200000 AND b.bar_time <= f.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}) AS out_pool_at_20m
                FROM features f LEFT JOIN bars b ON b.mint = f.mint AND b.bar_time > f.checkpoint_time
                GROUP BY f.mint, f.checkpoint_seconds, f.price
              )
              SELECT f.*, g.longest_gap_no_trades_s,
                     coalesce(l.launches_same_minute, 0) AS launches_same_minute,
                     coalesce(gr.graduations_same_hour, 0) AS graduations_same_hour,
                     extract(hour FROM to_timestamp(f.checkpoint_time / 1000.0)) AS hour_of_day_utc,
                     extract(dow FROM to_timestamp(f.checkpoint_time / 1000.0)) AS day_of_week_utc,
                     (f.checkpoint_time - f.first_bar_time) / 1000.0 AS age_at_checkpoint_s,
                     f.graduation_time IS NOT NULL AS graduated,
                     (f.checkpoint_time - f.graduation_time) / 1000.0 AS seconds_since_graduation,
                     o.out_peak_multiple, o.out_close_5m, o.out_close_20m, o.out_pool_at_5m, o.out_pool_at_20m,
                     f.checkpoint_time + 1200000 > {day_end} AS forward_window_truncated
              FROM features f LEFT JOIN gaps g USING (mint, checkpoint_seconds)
              LEFT JOIN launches l ON l.minute = date_trunc('minute', to_timestamp(f.first_bar_time / 1000.0))
              LEFT JOIN graduations gr ON gr.hour = date_trunc('hour', to_timestamp(f.checkpoint_time / 1000.0))
              LEFT JOIN outcomes o USING (mint, checkpoint_seconds)
            ) TO {quote(destination)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    worker_stats(day, "features")


def auc_sql(source: str, column: str, outcome: str, graduated_only: bool) -> dict[str, Any]:
    population = "AND graduated" if graduated_only else ""
    with duck(f"auc-{column}-{outcome}-{'grad' if graduated_only else 'all'}") as connection:
        row = connection.execute(
            f"""
            WITH usable AS (
              SELECT *, ntile(10) OVER (ORDER BY {outcome} DESC, mint) AS outcome_decile
              FROM read_parquet({source})
              WHERE {outcome} > 0 AND isfinite({outcome}) {population}
            ), chosen AS (
              SELECT {column}::DOUBLE AS value, outcome_decile = 1 AS winner
              FROM usable WHERE ({column} IS NOT NULL AND isfinite(({column})::DOUBLE)) AND (outcome_decile = 1 OR outcome_decile >= 7)
            ), grouped AS (
              SELECT value, count(*) AS group_count, sum(winner::INTEGER) AS winner_count FROM chosen GROUP BY value
            ), ranked AS (
              SELECT *, sum(group_count) OVER (ORDER BY value) - group_count + (group_count + 1) / 2.0 AS average_rank FROM grouped
            ), totals AS (
              SELECT coalesce(sum(winner_count), 0) AS winners, coalesce(sum(group_count - winner_count), 0) AS losers,
                     coalesce(sum(winner_count * average_rank), 0) AS winner_rank_sum FROM ranked
            )
            SELECT winners, losers, winner_rank_sum FROM totals
            """
        ).fetchone()
    winners, losers, rank_sum = row
    if not winners or not losers:
        return {"auc": None, "direction": "unavailable", "winners": winners, "losers": losers}
    raw = (rank_sum - winners * (winners + 1) / 2) / (winners * losers)
    return {"auc": max(raw, 1 - raw), "direction": "winners higher" if raw >= 0.5 else "winners lower", "winners": winners, "losers": losers}


def phase4(checkpoints: list[int]) -> list[dict[str, Any]]:
    chunks = [ROOT / "feature_chunks" / f"{day.isoformat()}.parquet" for day in days() if (ROOT / "feature_chunks" / f"{day.isoformat()}.parquet").is_file()]
    source = "[" + ", ".join(quote(path) for path in chunks) + "]"
    rows: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        checkpoint_source = f"(SELECT * FROM read_parquet({source}) WHERE checkpoint_seconds = {checkpoint})"
        # Materialize a compact checkpoint file so each AUC query avoids scanning other checkpoints.
        materialized = ROOT / "auc_inputs" / f"checkpoint_{checkpoint}.parquet"
        materialized.parent.mkdir(parents=True, exist_ok=True)
        with duck(f"auc-input-{checkpoint}") as connection:
            connection.execute(f"COPY {checkpoint_source} TO {quote(materialized)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        for population in ("all", "graduated_only"):
            for outcome in OUTCOMES:
                for column in CANDIDATE_COLUMNS:
                    result = auc_sql(quote(materialized), column, outcome, population == "graduated_only")
                    rows.append({
                        "checkpoint_seconds": checkpoint,
                        "population": population,
                        "outcome": outcome,
                        "column": column,
                        "normalized_auc": "" if result["auc"] is None else f"{result['auc']:.8f}",
                        "favored_direction": result["direction"],
                        "winner_count": result["winners"],
                        "loser_count": result["losers"],
                        "thin_sample_warning": bool(result["winners"] < 100 or result["losers"] < 100),
                    })
    write_csv(ROOT / "phase4_auc.csv", rows, list(rows[0]))
    return rows


def report(checkpoints: list[int], auc_rows: list[dict[str, Any]], started: float, completed_days: list[str]) -> None:
    phase0_rows = list(csv.DictReader((ROOT / "phase0_feasibility.csv").open(encoding="utf-8")))
    phase1_rows = list(csv.DictReader((ROOT / "phase1_survival.csv").open(encoding="utf-8")))
    phase2_rows = list(csv.DictReader((ROOT / "phase2_population.csv").open(encoding="utf-8")))
    source = "[" + ", ".join(quote(ROOT / "feature_chunks" / f"{day}.parquet") for day in completed_days) + "]"
    with duck("report") as connection:
        labels = ", ".join(quote(label_path(day)) for day in days())
        feature_source = f"read_parquet({source})" if completed_days else "(SELECT NULL::VARCHAR AS mint WHERE false)"
        label_match, total, truncated, duplicates = connection.execute(
            f"""
            WITH feature AS (SELECT * FROM {feature_source}), labels AS (SELECT DISTINCT mint FROM read_parquet([{labels}])),
            duplicate_rows AS (SELECT mint, checkpoint_seconds FROM feature GROUP BY 1, 2 HAVING count(*) > 1)
            SELECT count(*) FILTER (WHERE labels.mint IS NOT NULL), count(*), count(*) FILTER (WHERE forward_window_truncated), (SELECT count(*) FROM duplicate_rows)
            FROM feature LEFT JOIN labels USING (mint)
            """
        ).fetchone()
    feasibility = [row for row in phase0_rows if row["record_type"] == "candidate_feasibility"]
    computable = {row["candidate"] for row in feasibility if row["status"] == "computable"}
    verdicts: list[tuple[str, str, str, str]] = []
    for column in CANDIDATE_COLUMNS:
        values = [row for row in auc_rows if row["column"] == column and row["outcome"] != "out_peak_multiple" and row["population"] == "all" and row["normalized_auc"]]
        if not values:
            verdicts.append((column, "n/a", "n/a", "drop (unscorable)"))
            continue
        best = max(values, key=lambda row: float(row["normalized_auc"]))
        verdicts.append((column, best["normalized_auc"], f"{best['outcome']} @ {best['checkpoint_seconds']}s", "keep" if float(best["normalized_auc"]) > 0.55 else "drop"))
    stats_files = list((ROOT / "worker_stats").glob("*.json"))
    peak_rss = max((json.loads(path.read_text())["peak_rss_kb"] for path in stats_files), default=0)
    worker_mtimes = [path.stat().st_mtime for path in stats_files]
    observed_runtime_s = max(worker_mtimes) - min(worker_mtimes) if worker_mtimes else time.monotonic() - started
    schema = (ROOT / "schema_2026-04-18.txt").read_text(encoding="utf-8").strip().splitlines()
    wallet_coverage = {row["field"]: row for row in phase0_rows if row["record_type"] == "field_coverage_3day"}
    lines = [
        "# MT-765 Candidate Column Prove-Out",
        "",
        "This detached report reads only the enriched Parquets dated **2026-04-18 through 2026-05-18**. No day after May 18 was read. It does not write Hive or interact with PumpApi services.",
        "",
        "## Phase 0: Feasibility",
        "",
        "Bars carry five-second OHLCV/pool/trade aggregates and aggregate wallet fields only; they do not carry individual trade sizes, wallet identities, or buyer/seller counts. Price field: `close`; amount fields: `buy_volume_sol`, `sell_volume_sol`; pool field: `min_sol_in_pool`.",
        "",
        "### Full Apr 18 Schema",
        "",
        "```text",
        *schema,
        "```",
        "",
        "| candidate | status | required fields | reason |",
        "|---|---|---|---|",
        *[f"| {row['candidate']} | {row['status']} | {row['required_source_fields'] or 'n/a'} | {row['reason']} |" for row in feasibility],
        "",
        f"The prior 0.500 fields have distinct-value counts over the three-day sample: `unique_wallets_total={wallet_coverage['unique_wallets_total']['distinct_value_count']}`, `top10_holder_pct={wallet_coverage['top10_holder_pct']['distinct_value_count']}`, and `creator_holdings_pct={wallet_coverage['creator_holdings_pct']['distinct_value_count']}`. None is constant in this sample; participation candidates still remain blocked because neither buyer/seller identity nor directional wallet counts exist.",
        "",
        "## Phase 1: Checkpoints",
        "",
        "Checkpoints are empirical ages at which 90%, 75%, 50%, 25%, and 10% of the working population remain observed, capped at 60 minutes. Working population: at least one `min_sol_in_pool >= 1` bar and at least 10 total trades in the permitted window. No graduation filter was applied.",
        "",
        "| checkpoint age | alive | graduated | not graduated |",
        "|---:|---:|---:|",
        *[f"| {row['survival_target']} ({row['checkpoint_seconds']}s) | {row['alive_count']} | {row['graduated_alive_count']} | {row['not_graduated_alive_count']} |" for row in phase1_rows],
        "",
        "## Phase 2: Unfiltered Population",
        "",
        "`phase2_population.csv` contains total/graduated counts, group distributions for maximum pool, total trades, and observed lifespan, plus the complete pool/trade survivor grid. No floor is selected by this report.",
        "",
        "## Verdict List",
        "",
        "AUC is direction-normalized as `max(auc, 1 - auc)`. Winners are the top outcome decile; losers are the lower four deciles; the middle is excluded. Final verdict is ranked by the close outcomes only. `out_close_5m` and `out_close_20m` are exit proxies, not realizable returns; `out_pool_at_*` remains in the feature chunks for reachability review.",
        "",
        "| column | best close AUC | checkpoint/outcome | verdict |",
        "|---|---:|---|---|",
        *[f"| {column} | {score} | {where} | {verdict} |" for column, score, where, verdict in verdicts],
        "",
        "`phase4_auc.csv` reports all three outcomes for every computed column, at every checkpoint, both all-coins and graduated-only, with winner/loser group counts, direction, and a warning where either group has fewer than 100 rows.",
        "",
        "## Blockers And Handling",
        "",
        f"1. Day-boundary censoring: forward outcomes intentionally do not cross files. `{truncated:,}` checkpoint rows have a +20m target after their own day file and retain null outcomes rather than being silently dropped.",
        f"2. Forward windows crossing files: not read across files; the preceding count is the explicit truncation count.",
        "3. Null vs zero: source nulls remain null. No feature uses `coalesce(..., 0)` except context counts where no launches/graduations is a real zero. Per-field sample null counts are in `phase0_feasibility.csv`.",
        "4. Division by zero: all ratios use `nullif(denominator, 0)`; non-finite values are excluded from AUCs. No infinity is emitted.",
        f"5. Memory: DuckDB `memory_limit` was `{MEMORY_LIMIT}` with disk spill; max worker `ru_maxrss` was {peak_rss / 1024 / 1024:.2f} GiB. Each child was run under `run-capped 6G`.",
        f"6. Runtime: {len(completed_days)}/31 days completed; worker-artifact wall clock was {observed_runtime_s:.1f}s against the 3-hour cap.",
        "7. Constant columns: Phase 0 marks one-distinct-value source fields dead; dead/blocked candidates are not scored.",
        "8. Thin samples: `phase4_auc.csv.thin_sample_warning` flags any AUC with fewer than 100 winners or losers.",
        f"9. Duplicates: feature snapshot grouping produced `{duplicates}` duplicate mint/checkpoint keys; one row per coin/checkpoint is required.",
        f"10. Label join: `{label_match:,}` feature rows matched an MT-739 label and `{total - label_match:,}` did not. Labels are not used as candidate values because that would leak future state.",
        "",
        "## Temporal Enforcement",
        "",
        "Feature history joins `bar_time <= checkpoint_time`; outcomes join only `bar_time > checkpoint_time` in separate CTEs. The future `end_state` candidate is blocked rather than computed. Same-day forward-only scans are the reason near-midnight checkpoints are explicitly censored instead of borrowing May 19 data.",
    ]
    (ROOT / "VERDICT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_worker(flag: str, day: date, checkpoints: list[int] | None = None) -> None:
    command = ["run-capped", "6G", sys.executable, str(Path(__file__).resolve()), flag, day.isoformat(), "--output", str(ROOT)]
    if checkpoints:
        command.extend(["--checkpoints", ",".join(map(str, checkpoints))])
    subprocess.run(command, check=True)


def parse_checkpoints(value: str) -> list[int]:
    return [int(part) for part in value.split(",") if part]


def main() -> None:
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT)
    parser.add_argument("--summary-day", type=date.fromisoformat)
    parser.add_argument("--feature-day", type=date.fromisoformat)
    parser.add_argument("--checkpoints", type=parse_checkpoints, default=[])
    parser.add_argument("--finalize", action="store_true", help="score existing feature chunks and render the report")
    parser.add_argument("--phase0-only", action="store_true")
    parser.add_argument("--phase1-only", action="store_true")
    args = parser.parse_args()
    ROOT = args.output
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.summary_day:
        day_summary(args.summary_day)
        return
    if args.feature_day:
        feature_day(args.feature_day, args.checkpoints)
        return
    if args.phase0_only:
        phase0()
        return
    if args.phase1_only:
        phase1_phase2(args.checkpoints)
        return
    if args.finalize:
        completed = [day.isoformat() for day in days() if (ROOT / "feature_chunks" / f"{day.isoformat()}.parquet").is_file()]
        auc_rows = phase4(sorted(set(args.checkpoints)))
        report(args.checkpoints, auc_rows, time.monotonic(), completed)
        return
    started = time.monotonic()
    phase0()
    completed_summary: list[str] = []
    for day in days():
        if time.monotonic() - started >= RUNTIME_CAP_S:
            break
        run_worker("--summary-day", day)
        completed_summary.append(day.isoformat())
    if len(completed_summary) != len(days()):
        raise RuntimeError(f"runtime cap reached during summary: {len(completed_summary)}/31 days")
    checkpoints, _ = aggregate_population()
    phase1_phase2(checkpoints)
    feature_checkpoints = sorted(set(checkpoints))
    completed_features: list[str] = []
    for day in days():
        if time.monotonic() - started >= RUNTIME_CAP_S:
            break
        run_worker("--feature-day", day, feature_checkpoints)
        completed_features.append(day.isoformat())
    auc_rows = phase4(feature_checkpoints) if completed_features else []
    report(checkpoints, auc_rows, started, completed_features)
    print(f"completed_days={len(completed_features)}/31 checkpoints={checkpoints}", flush=True)


if __name__ == "__main__":
    main()
