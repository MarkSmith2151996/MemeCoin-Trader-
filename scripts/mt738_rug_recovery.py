#!/usr/bin/env python3
"""Rebuild PumpApi rug labels and measure post-removal archive recoveries."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path("/mnt/d/pumpapi-replay")
START = "2026-07-22"
END = "2026-08-21"
EXPECTED_LABELS = 726_420
POSITION_SIZE_SOL = 0.02


def dates(start: str, end: str) -> list[str]:
    current = datetime.fromisoformat(start).replace(tzinfo=UTC)
    last = datetime.fromisoformat(end).replace(tzinfo=UTC)
    result = []
    while current <= last:
        result.append(current.date().isoformat())
        current += timedelta(days=1)
    return result


def paths(root: Path, kind: str, start: str, end: str) -> list[str]:
    return [str(root / "derived" / kind / f"{date}.parquet") for date in dates(start, end)]


def source_list(values: list[str]) -> str:
    return ", ".join(json.dumps(value) for value in values)


def open_db() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '2GB'")
    connection.execute("SET threads = 2")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def rebuild_day_labels(
    connection: duckdb.DuckDBPyConnection, root: Path, date: str, output_path: Path
) -> None:
    ticks = source_list(paths(root, "ticks", date, date))
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE day_labels AS
        WITH ordered AS (
            SELECT mint, timestamp, signature, lower(action) AS action, sol_in_pool,
                   max(sol_in_pool) FILTER (WHERE sol_in_pool > 0) OVER (
                       PARTITION BY mint ORDER BY timestamp, signature
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS prior_peak_sol
            FROM read_parquet([{ticks}])
        ), candidates AS (
            SELECT *, row_number() OVER (PARTITION BY mint ORDER BY timestamp, signature) AS label_rank
            FROM ordered
            WHERE action = 'remove' OR sol_in_pool < prior_peak_sol * 0.01
        )
        SELECT mint, timestamp::BIGINT AS rug_timestamp, prior_peak_sol AS pool_sol_before,
               sol_in_pool AS pool_sol_after
        FROM candidates WHERE label_rank = 1
        """
    )
    connection.execute(
        f"COPY day_labels TO {json.dumps(str(output_path))} (HEADER, DELIMITER ',')"
    )


def rebuild_labels(root: Path, start: str, end: str, output_dir: Path) -> None:
    chunks = output_dir / "label_chunks"
    chunks.mkdir(exist_ok=True)
    for date in dates(start, end):
        destination = chunks / f"{date}.csv"
        if destination.exists():
            continue
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--root",
                str(root),
                "--output-dir",
                str(output_dir),
                "--worker-date",
                date,
            ],
            check=True,
        )


def label_count(connection: duckdb.DuckDBPyConnection) -> int:
    return int(connection.execute("SELECT count(DISTINCT mint) FROM labels").fetchone()[0])


def p99_ratio(connection: duckdb.DuckDBPyConnection, root: Path, start: str, end: str) -> float:
    enriched = source_list(paths(root, "enriched", start, end))
    return float(
        connection.execute(
            f"""
            WITH ratios AS (
                SELECT close / lag(close) OVER (PARTITION BY mint ORDER BY bar_time) AS ratio
                FROM read_parquet([{enriched}]) WHERE close > 0
            )
            SELECT quantile_cont(ratio, 0.99) FROM ratios WHERE ratio > 0 AND isfinite(ratio)
            """
        ).fetchone()[0]
    )


def recovery_rows(connection: duckdb.DuckDBPyConnection, root: Path, start: str, end: str, cap: float) -> list[dict[str, Any]]:
    enriched = source_list(paths(root, "enriched", start, end))
    rows = connection.execute(
        f"""
        SELECT labels.mint, labels.rug_timestamp,
               arg_max(close, bar_time) FILTER (WHERE bar_time <= rug_timestamp) AS rug_price,
               arg_max(max_sol_in_pool, bar_time) FILTER (WHERE bar_time <= rug_timestamp) AS rug_pool_sol,
               arg_min(close, bar_time) FILTER (WHERE bar_time > rug_timestamp) AS post_price,
               arg_min(max_sol_in_pool, bar_time) FILTER (WHERE bar_time > rug_timestamp) AS post_pool_sol,
               sum(trade_count) FILTER (WHERE bar_time > rug_timestamp - 60000 AND bar_time <= rug_timestamp) AS final_minute_trades,
               min(bar_time) FILTER (WHERE graduated_this_bar) AS graduation_time,
               arg_min(market_cap_usd, bar_time) AS first_observed_market_cap_usd
        FROM labels LEFT JOIN read_parquet([{enriched}]) USING (mint)
        GROUP BY labels.mint, labels.rug_timestamp
        """
    ).fetchall()
    result = []
    for mint, timestamp, rug_price, rug_pool, post_price, post_pool, final_trades, graduation, first_mcap in rows:
        observed = all(value is not None and value > 0 for value in (rug_price, post_price, post_pool))
        recovery = None
        if observed:
            proceeds = min(POSITION_SIZE_SOL * min(float(post_price) / float(rug_price), cap), float(post_pool) * 0.25)
            recovery = proceeds / POSITION_SIZE_SOL
        result.append({
            "mint": mint, "rug_timestamp": int(timestamp), "rug_price": rug_price,
            "rug_pool_sol": rug_pool, "post_price": post_price, "post_pool_sol": post_pool,
            "final_minute_trades": final_trades, "graduation_to_rug_s": None if graduation is None else (int(timestamp) - int(graduation)) / 1000,
            "first_observed_market_cap_usd": first_mcap, "recovery_fraction": recovery,
        })
    return result


def quantile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    position = (len(ordered) - 1) * fraction
    lower, upper = int(position), min(int(position) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summary(values: list[float]) -> dict[str, float | int]:
    return {
        "n": len(values), "mean": sum(values) / len(values), "median": quantile(values, .5),
        "p10": quantile(values, .1), "p25": quantile(values, .25), "p75": quantile(values, .75),
        "p90": quantile(values, .9), "zero_share": sum(value == 0 for value in values) / len(values),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", default=START)
    parser.add_argument("--end", default=END)
    parser.add_argument("--worker-date", help="Internal one-day label worker.")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.worker_date:
        with open_db() as connection:
            rebuild_day_labels(
                connection,
                args.root,
                args.worker_date,
                args.output_dir / "label_chunks" / f"{args.worker_date}.csv",
            )
        return

    rebuild_labels(args.root, args.start, args.end, args.output_dir)
    with open_db() as connection:
        chunks = source_list(
            [str(args.output_dir / "label_chunks" / f"{date}.csv") for date in dates(args.start, args.end)]
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE labels AS
            SELECT mint, rug_timestamp::BIGINT AS rug_timestamp, pool_sol_before, pool_sol_after
            FROM read_csv([{chunks}], header=true)
            QUALIFY row_number() OVER (PARTITION BY mint ORDER BY rug_timestamp) = 1
            """
        )
        count = label_count(connection)
        connection.execute(f"COPY labels TO {json.dumps(str(args.output_dir / 'reimplemented_rug_labels.csv'))} (HEADER, DELIMITER ',')")
        if count != EXPECTED_LABELS:
            raise SystemExit(f"Label reproduction failed: {count:,} labels, expected {EXPECTED_LABELS:,}")
        cap = p99_ratio(connection, args.root, args.start, args.end)
        rows = recovery_rows(connection, args.root, args.start, args.end, cap)
    with (args.output_dir / "rug_recovery_rows.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    observed = [float(row["recovery_fraction"]) for row in rows if row["recovery_fraction"] is not None]
    payload = {"label_count": count, "p99_trigger_relative_cap": cap, "observed_recovery": summary(observed)}
    (args.output_dir / "rug_recovery_summary.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
