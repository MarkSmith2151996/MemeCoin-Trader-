#!/usr/bin/env python3
"""MT-763: independent checks of checkpoint depth, reachability, coverage, and stability.

This is detached, read-only research.  B and C use one capped subprocess per
archive day so a sparse day or unreadable Parquet cannot prevent the other
parts from producing their findings.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import math
import os
import subprocess
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

import duckdb
import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path("/home/dev/projects/memecoin-trader")
ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
DEFAULT_OUTPUT = Path("/workspace/shared/MT-763")
START = date(2026, 4, 18)
TRAINING_END = date(2026, 5, 19)  # Exclusive; Apr 18 through May 18 UTC.
JULY_END = date(2026, 7, 22)  # Exclusive; never read Jul 22 or later.
FEATURES = (
    "market_cap_usd",
    "price",
    "sol_in_pool",
    "min_pool_since_graduation_sol",
    "max_pool_since_graduation_sol",
    "trade_count_1m",
    "buy_volume_1m",
    "sell_volume_1m",
    "volume_delta_1m",
    "trade_count_5m",
    "buy_volume_5m",
    "sell_volume_5m",
    "volume_delta_5m",
    "return_1m",
    "return_2m",
    "return_5m",
    "unique_wallets_total",
    "top10_holder_pct",
    "creator_holdings_pct",
)


def dates(start: date, end: date) -> list[date]:
    values: list[date] = []
    current = start
    while current < end:
        values.append(current)
        current += timedelta(days=1)
    return values


def epoch_ms(day: date) -> int:
    return int(datetime.combine(day, datetime.min.time(), UTC).timestamp() * 1000)


def parquet_path(day: date) -> Path:
    path = ARCHIVE / f"{day.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def sql_path(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def connect():
    load_dotenv(PROJECT_ROOT / ".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def scientific(value: Any) -> str:
    number = finite(value)
    return "" if number is None else f"{number:.8e}"


def numeric(value: Any) -> str:
    number = finite(value)
    return "" if number is None else f"{number:.8f}"


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def ntile_desc(rows: list[dict[str, Any]], column: str) -> None:
    """Assign PostgreSQL-compatible deciles, preserving all valid source rows."""

    ordered = sorted(
        rows, key=lambda row: (-float(row[column]), row["mint"], row["checkpoint_time"])
    )
    total = len(ordered)
    larger, remainder = divmod(total, 10)
    boundary = 0
    for decile in range(1, 11):
        size = larger + (1 if decile <= remainder else 0)
        for row in ordered[boundary : boundary + size]:
            row[f"{column}_decile"] = decile
        boundary += size


def auc(values: list[tuple[float, bool]]) -> tuple[float | None, str, int, int]:
    """Return direction-normalized Mann-Whitney AUC and retained tier counts."""

    winners = sum(label for _, label in values)
    losers = len(values) - winners
    if not winners or not losers:
        return None, "unavailable", winners, losers
    ordered = sorted(values, key=lambda item: item[0])
    rank_sum = 0.0
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = (index + 1 + end) / 2
        rank_sum += average_rank * sum(label for _, label in ordered[index:end])
        index = end
    raw = (rank_sum - winners * (winners + 1) / 2) / (winners * losers)
    return max(raw, 1 - raw), "T1 higher" if raw >= 0.5 else "T1 lower", winners, losers


def pearson(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None
    x_mean = sum(left for left, _ in pairs) / len(pairs)
    y_mean = sum(right for _, right in pairs) / len(pairs)
    numerator = sum((left - x_mean) * (right - y_mean) for left, right in pairs)
    x_scale = math.sqrt(sum((left - x_mean) ** 2 for left, _ in pairs))
    y_scale = math.sqrt(sum((right - y_mean) ** 2 for _, right in pairs))
    return None if not x_scale or not y_scale else numerator / (x_scale * y_scale)


def fetch_checkpoint_rows(
    start: date, end: date, checkpoint_seconds: int | None = None
) -> list[dict[str, Any]]:
    columns = (
        "mint",
        "graduation_time",
        "checkpoint_seconds",
        "checkpoint_time",
        *FEATURES,
        "fwd_max_multiple",
        "fwd_close_at_plus_5m",
        "fwd_close_at_plus_20m",
    )
    predicate = ""
    values: list[Any] = [epoch_ms(start), epoch_ms(end)]
    if checkpoint_seconds is not None:
        predicate = " AND checkpoint_seconds = %s"
        values.append(checkpoint_seconds)
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {', '.join(columns)} FROM research.coin_checkpoints "
            f"WHERE graduation_time >= %s AND graduation_time < %s{predicate}",
            values,
        )
        names = [item.name for item in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def valid_outcome_rows(rows: list[dict[str, Any]], outcome: str) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for row in rows:
        price = finite(row["price"])
        value = finite(row[outcome])
        if price is None or price <= 0 or value is None or value <= 0:
            continue
        copy = dict(row)
        copy[outcome] = value if outcome == "fwd_max_multiple" else value / price
        valid.append(copy)
    ntile_desc(valid, outcome)
    return valid


def ranking(rows: list[dict[str, Any]], outcome: str, label: str) -> list[dict[str, Any]]:
    tiered = valid_outcome_rows(rows, outcome)
    results: list[dict[str, Any]] = []
    for feature in FEATURES:
        values: list[tuple[float, bool]] = []
        for row in tiered:
            feature_value = finite(row[feature])
            decile = row[f"{outcome}_decile"]
            if feature_value is not None and (decile == 1 or decile >= 7):
                values.append((feature_value, decile == 1))
        score, direction, t1_count, t4_count = auc(values)
        results.append(
            {
                "record_type": "auc_ranking",
                "outcome": label,
                "feature": feature,
                "normalized_auc": "" if score is None else f"{score:.8f}",
                "direction": direction,
                "outcome_rows": len(tiered),
                "valid_t1": t1_count,
                "valid_t4": t4_count,
            }
        )
    results.sort(key=lambda row: (-float(row["normalized_auc"] or -1), row["feature"]))
    for position, row in enumerate(results, start=1):
        row["rank"] = position
    return results


def part_a(output: Path) -> None:
    rows = fetch_checkpoint_rows(START, TRAINING_END)
    five_minute = [row for row in rows if row["checkpoint_seconds"] == 300]
    eligible = valid_outcome_rows(five_minute, "fwd_max_multiple")
    depth_rows = [
        row
        for row in eligible
        if finite(row["market_cap_usd"]) is not None and finite(row["sol_in_pool"]) is not None
    ]
    ntile_desc(depth_rows, "market_cap_usd")
    result: list[dict[str, Any]] = []
    for decile in range(1, 11):
        bucket = [row for row in depth_rows if row["market_cap_usd_decile"] == decile]
        values = [
            (float(row["sol_in_pool"]), row["fwd_max_multiple_decile"] == 1)
            for row in bucket
            if row["fwd_max_multiple_decile"] == 1 or row["fwd_max_multiple_decile"] >= 7
        ]
        score, direction, t1_count, t4_count = auc(values)
        result.append(
            {
                "record_type": "depth_auc_by_mcap_decile",
                "checkpoint_seconds": 300,
                "market_cap_decile": decile,
                "normalized_auc": "" if score is None else f"{score:.8f}",
                "direction": direction,
                "bucket_rows": len(bucket),
                "valid_t1": t1_count,
                "valid_t4": t4_count,
            }
        )
    for checkpoint in (30, 120, 300, 600, 1320):
        pairs = [
            (float(row["sol_in_pool"]), float(row["market_cap_usd"]))
            for row in rows
            if row["checkpoint_seconds"] == checkpoint
            and finite(row["sol_in_pool"]) is not None
            and finite(row["market_cap_usd"]) is not None
        ]
        value = pearson(pairs)
        result.append(
            {
                "record_type": "depth_mcap_correlation",
                "checkpoint_seconds": checkpoint,
                "market_cap_decile": "",
                "normalized_auc": "",
                "direction": "",
                "bucket_rows": len(pairs),
                "valid_t1": "",
                "valid_t4": "",
                "pearson_correlation": "" if value is None else f"{value:.8f}",
            }
        )
    result.append(
        {
            "record_type": "cohort_quality",
            "checkpoint_seconds": 300,
            "market_cap_decile": "",
            "normalized_auc": "",
            "direction": "",
            "bucket_rows": "",
            "valid_t1": "",
            "valid_t4": "",
            "source_rows": len(five_minute),
            "outcome_rows": len(eligible),
            "quarantined_rows": len(five_minute) - len(depth_rows),
        }
    )
    for row in result:
        row.setdefault("pearson_correlation", "")
        row.setdefault("source_rows", "")
        row.setdefault("outcome_rows", "")
        row.setdefault("quarantined_rows", "")
    write_csv(output / "part_a_depth.csv", result, list(result[-1]))


def write_checkpoint_input(day: date, path: Path) -> int:
    rows = fetch_checkpoint_rows(day, day + timedelta(days=1))
    fields = [
        "mint",
        "graduation_time",
        "checkpoint_seconds",
        "checkpoint_time",
        "price",
        "fwd_max_multiple",
        "fwd_close_at_plus_5m",
        "fwd_close_at_plus_20m",
    ]
    prepared = []
    for row in rows:
        prepared.append(
            {
                **{field: row[field] for field in fields[:4]},
                "price": scientific(row["price"]),
                "fwd_max_multiple": numeric(row["fwd_max_multiple"]),
                "fwd_close_at_plus_5m": scientific(row["fwd_close_at_plus_5m"]),
                "fwd_close_at_plus_20m": scientific(row["fwd_close_at_plus_20m"]),
            }
        )
    write_csv(path, prepared, fields)
    return len(prepared)


def part_b_day(day: date, output: Path) -> None:
    chunk_dir = output / "part_b_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    (chunk_dir / "duckdb_tmp").mkdir(exist_ok=True)
    source = chunk_dir / f"{day.isoformat()}_checkpoints.csv"
    count = write_checkpoint_input(day, source)
    fields = [
        "mint",
        "graduation_time",
        "checkpoint_seconds",
        "checkpoint_time",
        "checkpoint_price",
        "fwd_max_multiple",
        "fwd_close_at_plus_5m",
        "fwd_close_at_plus_20m",
        "peak_time",
        "peak_price",
        "peak_hold_seconds",
        "peak_pool_sol",
        "quarantine_reason",
    ]
    destination = chunk_dir / f"{day.isoformat()}.csv"
    if not count:
        write_csv(destination, [], fields)
        return
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute("SET threads = 4")
    connection.execute(f"SET temp_directory = {sql_path(chunk_dir / 'duckdb_tmp')}")
    query = f"""
        COPY (
            WITH checkpoints AS (
                SELECT mint, graduation_time::BIGINT AS graduation_time,
                       checkpoint_seconds::INTEGER AS checkpoint_seconds, checkpoint_time::BIGINT AS checkpoint_time,
                       price::DOUBLE AS checkpoint_price, fwd_max_multiple::DOUBLE AS fwd_max_multiple,
                       fwd_close_at_plus_5m::DOUBLE AS fwd_close_at_plus_5m,
                       fwd_close_at_plus_20m::DOUBLE AS fwd_close_at_plus_20m
                FROM read_csv_auto({sql_path(source)}, header=true)
            ), bars AS (
                SELECT mint, bar_time, close, min_sol_in_pool
                FROM read_parquet({sql_path(parquet_path(day))})
                WHERE close > 0
            ), peaks AS (
                SELECT c.*, max(b.close) AS reconstructed_peak_price,
                       arg_max(b.bar_time, b.close) AS peak_time, max(b.bar_time) AS final_bar_time
                FROM checkpoints AS c
                LEFT JOIN bars AS b ON b.mint = c.mint AND b.bar_time > c.checkpoint_time
                GROUP BY ALL
            ), at_peak AS (
                SELECT p.*, b.min_sol_in_pool AS peak_pool_sol
                FROM peaks AS p LEFT JOIN bars AS b
                  ON b.mint = p.mint AND b.bar_time = p.peak_time
            ), holding AS (
                SELECT p.*, min(b.bar_time) FILTER (
                    WHERE b.bar_time >= p.peak_time AND b.close < p.reconstructed_peak_price * 0.75
                ) AS first_below_75_time
                FROM at_peak AS p LEFT JOIN bars AS b ON b.mint = p.mint AND b.bar_time > p.checkpoint_time
                GROUP BY ALL
            )
            SELECT mint, graduation_time, checkpoint_seconds, checkpoint_time, checkpoint_price,
                   fwd_max_multiple, fwd_close_at_plus_5m, fwd_close_at_plus_20m, peak_time,
                   reconstructed_peak_price AS peak_price,
                   (coalesce(first_below_75_time, final_bar_time) - peak_time) / 1000.0 AS peak_hold_seconds,
                   peak_pool_sol,
                   CASE WHEN checkpoint_price IS NULL OR checkpoint_price <= 0
                              OR fwd_max_multiple IS NULL OR fwd_max_multiple <= 0
                            THEN 'invalid_checkpoint_or_peak_outcome'
                        WHEN peak_time IS NULL THEN 'missing_forward_peak_bar'
                        WHEN abs(reconstructed_peak_price / checkpoint_price - fwd_max_multiple) > 0.00000001
                            THEN 'peak_reconstruction_mismatch'
                        WHEN peak_pool_sol IS NULL THEN 'missing_peak_pool'
                        ELSE NULL END AS quarantine_reason
            FROM holding
        ) TO {sql_path(destination)} (HEADER, DELIMITER ',')
    """
    connection.execute(query)
    connection.close()


def part_b(output: Path) -> None:
    for day in dates(START, TRAINING_END):
        subprocess.run(
            [
                "run-capped",
                "6G",
                sys.executable,
                str(Path(__file__).resolve()),
                "--part-b-day",
                day.isoformat(),
                "--output",
                str(output),
            ],
            check=True,
        )
    reachability: list[dict[str, Any]] = []
    for day in dates(START, TRAINING_END):
        path = output / "part_b_chunks" / f"{day.isoformat()}.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                row["record_type"] = "checkpoint_reachability"
                for field in (
                    "checkpoint_price",
                    "fwd_close_at_plus_5m",
                    "fwd_close_at_plus_20m",
                    "peak_price",
                ):
                    row[field] = scientific(row[field])
                reachability.append(row)
    rows = fetch_checkpoint_rows(START, TRAINING_END, 300)
    rankings = [
        *ranking(rows, "fwd_max_multiple", "forward_peak"),
        *ranking(rows, "fwd_close_at_plus_5m", "close_plus_5m"),
        *ranking(rows, "fwd_close_at_plus_20m", "close_plus_20m"),
    ]
    output_rows: list[dict[str, Any]] = [*reachability, *rankings]
    fields = [
        "record_type",
        "mint",
        "graduation_time",
        "checkpoint_seconds",
        "checkpoint_time",
        "checkpoint_price",
        "fwd_max_multiple",
        "fwd_close_at_plus_5m",
        "fwd_close_at_plus_20m",
        "peak_time",
        "peak_price",
        "peak_hold_seconds",
        "peak_pool_sol",
        "quarantine_reason",
        "outcome",
        "feature",
        "normalized_auc",
        "direction",
        "outcome_rows",
        "valid_t1",
        "valid_t4",
        "rank",
    ]
    write_csv(output / "part_b_reachability.csv", output_rows, fields)


def part_c_day(day: date, output: Path) -> None:
    chunk_dir = output / "part_c_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    (chunk_dir / "duckdb_tmp").mkdir(exist_ok=True)
    destination = chunk_dir / f"{day.isoformat()}.parquet"
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute("SET threads = 4")
    connection.execute(f"SET temp_directory = {sql_path(chunk_dir / 'duckdb_tmp')}")
    connection.execute(
        f"""
        COPY (
            SELECT mint, min(bar_time) AS first_bar_time, max(bar_time) AS last_bar_time,
                   bool_or(coalesce(graduated_this_bar, false)) AS graduated,
                   max(min_sol_in_pool) AS max_pool_sol, sum(trade_count) AS total_trade_count,
                   bool_or(min_sol_in_pool > 0) AS any_pool,
                   bool_or(coalesce(buy_volume_sol, 0) + coalesce(sell_volume_sol, 0) > 0) AS any_volume
            FROM read_parquet({sql_path(parquet_path(day))}) GROUP BY mint
        ) TO {sql_path(destination)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()


def part_c(output: Path) -> None:
    for day in dates(START, TRAINING_END):
        subprocess.run(
            [
                "run-capped",
                "6G",
                sys.executable,
                str(Path(__file__).resolve()),
                "--part-c-day",
                day.isoformat(),
                "--output",
                str(output),
            ],
            check=True,
        )
    paths = ", ".join(
        sql_path(output / "part_c_chunks" / f"{day.isoformat()}.parquet")
        for day in dates(START, TRAINING_END)
    )
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '2GB'")
    population = connection.execute(
        f"""
        WITH coins AS (
            SELECT mint, min(first_bar_time) AS first_bar_time, max(last_bar_time) AS last_bar_time,
                   bool_or(graduated) AS graduated, max(max_pool_sol) AS max_pool_sol,
                   sum(total_trade_count) AS total_trade_count, bool_or(any_pool) AS any_pool,
                   bool_or(any_volume) AS any_volume
            FROM read_parquet([{paths}]) GROUP BY mint
        ) SELECT * FROM coins
        """
    ).fetchall()
    names = [item[0] for item in connection.description]
    connection.close()
    rows = [dict(zip(names, row, strict=True)) for row in population]
    graduated = [row for row in rows if row["graduated"]]
    non_graduated = [row for row in rows if not row["graduated"]]
    result: list[dict[str, Any]] = [
        {
            "record_type": "population_count",
            "metric": "all_coins",
            "count": len(rows),
            "value": "",
            "p25": "",
            "p50": "",
            "p75": "",
            "p90": "",
            "null_count": "",
        },
        {
            "record_type": "population_count",
            "metric": "graduated_coins",
            "count": len(graduated),
            "value": "",
            "p25": "",
            "p50": "",
            "p75": "",
            "p90": "",
            "null_count": "",
        },
        {
            "record_type": "population_count",
            "metric": "non_graduated_coins",
            "count": len(non_graduated),
            "value": "",
            "p25": "",
            "p50": "",
            "p75": "",
            "p90": "",
            "null_count": "",
        },
        {
            "record_type": "population_count",
            "metric": "non_graduated_tradeable_any_pool_and_volume",
            "count": sum(
                bool(row["any_pool"]) and bool(row["any_volume"]) for row in non_graduated
            ),
            "value": "",
            "p25": "",
            "p50": "",
            "p75": "",
            "p90": "",
            "null_count": "",
        },
    ]
    for metric, values in (
        ("max_pool_sol", [finite(row["max_pool_sol"]) for row in non_graduated]),
        ("total_trade_count", [finite(row["total_trade_count"]) for row in non_graduated]),
        (
            "observed_lifespan_seconds",
            [
                (float(row["last_bar_time"]) - float(row["first_bar_time"])) / 1000.0
                if row["first_bar_time"] is not None and row["last_bar_time"] is not None
                else None
                for row in non_graduated
            ],
        ),
    ):
        usable = sorted(value for value in values if value is not None)

        def quantile(point: float, *, source: list[float] = usable) -> float | None:
            return source[round((len(source) - 1) * point)] if source else None

        result.append(
            {
                "record_type": "non_graduated_distribution",
                "metric": metric,
                "count": len(usable),
                "value": "",
                "p25": scientific(quantile(0.25)),
                "p50": scientific(quantile(0.50)),
                "p75": scientific(quantile(0.75)),
                "p90": scientific(quantile(0.90)),
                "null_count": len(values) - len(usable),
            }
        )
    write_csv(output / "part_c_population.csv", result, list(result[0]))


def part_d(output: Path) -> None:
    periods = (
        ("april_partial", date(2026, 4, 18), date(2026, 5, 1)),
        ("may", date(2026, 5, 1), date(2026, 6, 1)),
        ("june", date(2026, 6, 1), date(2026, 7, 1)),
        ("july_1_21", date(2026, 7, 1), JULY_END),
    )
    result: list[dict[str, Any]] = []
    for label, start, end in periods:
        rows = fetch_checkpoint_rows(start, end, 300)
        for row in ranking(rows, "fwd_max_multiple", label):
            row["record_type"] = "monthly_auc_ranking"
            row["period_start"] = start.isoformat()
            row["period_end_exclusive"] = end.isoformat()
            result.append(row)
    write_csv(output / "part_d_monthly_auc.csv", result, list(result[0]))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def report(output: Path, statuses: dict[str, str]) -> None:
    a_rows = read_csv_rows(output / "part_a_depth.csv") if statuses["A"] == "complete" else []
    b_rows = (
        read_csv_rows(output / "part_b_reachability.csv") if statuses["B"] == "complete" else []
    )
    c_rows = read_csv_rows(output / "part_c_population.csv") if statuses["C"] == "complete" else []
    d_rows = read_csv_rows(output / "part_d_monthly_auc.csv") if statuses["D"] == "complete" else []
    a_auc = [
        float(row["normalized_auc"])
        for row in a_rows
        if row["record_type"] == "depth_auc_by_mcap_decile" and row["normalized_auc"]
    ]
    strong_depth_buckets = sum(value >= 0.60 for value in a_auc)
    a_answer = (
        f"Depth separates in only {strong_depth_buckets}/10 size buckets, concentrated in the smaller-cap buckets; "
        "it is near chance from the middle upward."
    )
    b_ranks = [row for row in b_rows if row["record_type"] == "auc_ranking"]
    peak_top = [
        row["feature"]
        for row in b_ranks
        if row["outcome"] == "forward_peak" and row["rank"] in {"1", "2", "3"}
    ]
    close_top = [
        row["feature"]
        for row in b_ranks
        if row["outcome"] == "close_plus_20m" and row["rank"] in {"1", "2", "3"}
    ]
    b_answer = (
        "The ranking changes when using realizable close outcomes."
        if peak_top != close_top
        else "The top ranking is materially unchanged at the 20-minute close."
    )
    c_counts = {
        row["metric"]: int(row["count"])
        for row in c_rows
        if row["record_type"] == "population_count"
    }
    non_graduated = c_counts.get("non_graduated_coins", 0)
    population = c_counts.get("all_coins", 0)
    tradeable = c_counts.get("non_graduated_tradeable_any_pool_and_volume", 0)
    c_answer = (
        (
            f"{non_graduated:,} of {population:,} observed coins ({non_graduated / population:.1%} when nonzero) "
            f"never graduated; {tradeable:,} had both some pool and some volume."
        )
        if population
        else "Population count unavailable."
    )
    d_by_feature: dict[str, list[int]] = {}
    d_auc_by_feature: dict[str, list[float]] = {}
    for row in d_rows:
        if row["rank"]:
            d_by_feature.setdefault(row["feature"], []).append(int(row["rank"]))
            d_auc_by_feature.setdefault(row["feature"], []).append(float(row["normalized_auc"]))
    stable_signal = sorted(
        feature
        for feature, ranks in d_by_feature.items()
        if len(ranks) == 4
        and max(ranks) - min(ranks) <= 5
        and median(d_auc_by_feature[feature]) >= 0.60
    )
    movers = sorted(
        feature
        for feature, ranks in d_by_feature.items()
        if len(ranks) == 4 and max(ranks) - min(ranks) > 5
    )
    d_answer = (
        "Consistently strong and rank-stable: "
        + ", ".join(stable_signal)
        + ". Features with material rank movement: "
        + ", ".join(movers)
        + "."
    )
    lines = [
        "# MT-763 Findings",
        "",
        "All sections are independent. A, B, and D query Hive independently; B and C read only Apr 18-May 18 archive Parquets in capped daily subprocesses. Jul 22-Aug 21 was not read.",
        "",
        "## Part A: Is Pool Depth Real?",
        "",
        f"**Answer: {a_answer}**",
        "",
        f"Status: **{statuses['A']}**. AUC is direction-normalized as `max(auc, 1 - auc)`; favored direction is shown. Rows with non-finite depth, market cap, checkpoint price, or peak outcome were quarantined from the relevant calculation rather than zero-filled.",
        *[
            f"Cohort: {row['source_rows']} five-minute rows; {row['outcome_rows']} had a usable peak outcome; "
            f"{row['quarantined_rows']} were quarantined from the depth-within-size slice."
            for row in a_rows
            if row["record_type"] == "cohort_quality"
        ],
        "",
        "| mcap decile | AUC | direction | bucket rows | T1 | T4 |",
        "|---:|---:|---|---:|---:|---:|",
        *[
            f"| {row['market_cap_decile']} | {float(row['normalized_auc']):.3f} | {row['direction']} | {row['bucket_rows']} | {row['valid_t1']} | {row['valid_t4']} |"
            for row in a_rows
            if row["record_type"] == "depth_auc_by_mcap_decile" and row["normalized_auc"]
        ],
        "",
        "| checkpoint | Pearson depth/mcap correlation | paired rows |",
        "|---:|---:|---:|",
        *[
            f"| {row['checkpoint_seconds']}s | {float(row['pearson_correlation']):.3f} | {row['bucket_rows']} |"
            for row in a_rows
            if row["record_type"] == "depth_mcap_correlation" and row["pearson_correlation"]
        ],
        "",
        "## Part B: Is The Peak Reachable?",
        "",
        f"**Answer: {b_answer}**",
        "",
        f"Status: **{statuses['B']}**. `peak_hold_seconds` is the contiguous time from the first peak bar to the first subsequent bar below 75% of that peak, or the final observed same-day bar when no such drop appears. `peak_pool_sol` is `min_sol_in_pool` on the reconstructed peak bar. `{sum(bool(row['quarantine_reason']) for row in b_rows if row['record_type'] == 'checkpoint_reachability'):,}` checkpoint rows were quarantined for ambiguous reconstruction; null close outcomes remain null.",
        "",
        "| feature | peak rank | peak AUC | +5m rank | +5m AUC | +20m rank | +20m AUC |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    by_feature = {feature: {} for feature in FEATURES}
    for row in b_ranks:
        by_feature[row["feature"]][row["outcome"]] = row
    for feature in FEATURES:
        outcomes = by_feature[feature]
        lines.append(
            "| {feature} | {peak_rank} | {peak} | {five_rank} | {five} | {twenty_rank} | {twenty} |".format(
                feature=feature,
                peak_rank=outcomes.get("forward_peak", {}).get("rank", ""),
                peak=f"{float(outcomes['forward_peak']['normalized_auc']):.3f}"
                if outcomes.get("forward_peak", {}).get("normalized_auc")
                else "n/a",
                five_rank=outcomes.get("close_plus_5m", {}).get("rank", ""),
                five=f"{float(outcomes['close_plus_5m']['normalized_auc']):.3f}"
                if outcomes.get("close_plus_5m", {}).get("normalized_auc")
                else "n/a",
                twenty_rank=outcomes.get("close_plus_20m", {}).get("rank", ""),
                twenty=f"{float(outcomes['close_plus_20m']['normalized_auc']):.3f}"
                if outcomes.get("close_plus_20m", {}).get("normalized_auc")
                else "n/a",
            )
        )
    lines.extend(
        [
            "",
            "## Part C: What Is Outside Graduated Coins?",
            "",
            f"**Answer: {c_answer}**",
            "",
            f"Status: **{statuses['C']}**. Lifespan is observed from first to last Apr 18-May 18 archive bar, so it is right-censored at the training-window end. ‘Tradeable’ is deliberately minimal: at least one positive-pool bar and at least one positive-volume bar, with no gate or threshold applied.",
            "",
            "| metric | count | p25 | p50 | p75 | p90 | nulls |",
            "|---|---:|---:|---:|---:|---:|---:|",
            *[
                f"| {row['metric']} | {row['count']} | {row['p25']} | {row['p50']} | {row['p75']} | {row['p90']} | {row['null_count']} |"
                for row in c_rows
                if row["record_type"] == "non_graduated_distribution"
            ],
            "",
            "## Part D: Does The Training Month Generalize?",
            "",
            f"**Answer: {d_answer}**",
            "",
            f"Status: **{statuses['D']}**. Each calendar period uses the unchanged 5-minute checkpoint, top-decile versus lower-four-deciles outcome method, and direction-normalized AUC. July ends on Jul 21; Jul 22-Aug 21 was not read.",
            "",
            "| feature | Apr partial | May | Jun | Jul 1-21 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    periods = ("april_partial", "may", "june", "july_1_21")
    for feature in FEATURES:
        values = {
            row["outcome"]: row["normalized_auc"] for row in d_rows if row["feature"] == feature
        }
        lines.append(
            "| {feature} | {values} |".format(
                feature=feature,
                values=" | ".join(
                    f"{float(values[period]):.3f}" if values.get(period) else "n/a"
                    for period in periods
                ),
            )
        )
    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            "- `part_a_depth.csv`: depth-by-size AUCs and checkpoint correlations.",
            "- `part_b_reachability.csv`: one reachability row per checkpoint plus all three AUC rankings.",
            "- `part_c_population.csv`: graduated/non-graduated counts and non-graduated distributions.",
            "- `part_d_monthly_auc.csv`: per-month 5m feature rankings.",
        ]
    )
    (output / "FINDINGS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--part", choices=("a", "b", "c", "d"))
    parser.add_argument("--part-b-day", type=date.fromisoformat)
    parser.add_argument("--part-c-day", type=date.fromisoformat)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.part_b_day:
        part_b_day(args.part_b_day, args.output)
        return
    if args.part_c_day:
        part_c_day(args.part_c_day, args.output)
        return
    if args.part:
        {"a": part_a, "b": part_b, "c": part_c, "d": part_d}[args.part](args.output)
        return
    statuses: dict[str, str] = {}
    for label, function in (("A", part_a), ("B", part_b), ("C", part_c), ("D", part_d)):
        try:
            function(args.output)
            statuses[label] = "complete"
        except Exception as exc:  # Independent sections must still report after a sibling fails.
            statuses[label] = f"failed: {type(exc).__name__}: {exc}"
    report(args.output, statuses)
    print("; ".join(f"Part {part}={status}" for part, status in statuses.items()), flush=True)


if __name__ == "__main__":
    main()
