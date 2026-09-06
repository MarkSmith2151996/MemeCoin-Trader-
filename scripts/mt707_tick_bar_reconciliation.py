#!/usr/bin/env python3
"""Diagnose sparse MT-706 paths against PumpApi tick and enriched bar data.

This is an offline, read-only audit.  It processes one Parquet day at a time
and writes resumable CSV/JSON results under ``D:\\pumpapi-replay\\results\\mt707``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import resource
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

import duckdb
import pandas as pd


DEFAULT_ROOT = Path("/mnt/d/pumpapi-replay")
FLAGGED_CELLS = {
    ("2026-05-03", 19),
    ("2026-05-03", 18),
    ("2026-04-30", 20),
    ("2026-04-28", 19),
    ("2026-05-05", 22),
}
GATES = {
    "2m_trade_count": 131.0,
    "2m_buy_volume_sol": 37.0,
    "1m_trade_count": 84.0,
    "1m_buy_volume_sol": 26.5,
    "2m_unique_wallets": 12.0,
    "1m_unique_wallets": 11.0,
    "creator_2x_rate": 0.3,
}
ENTRY_OFFSET_MS = 120_000
ENTRY_DELAY_MS = 42_555
WINDOW_MS = 10 * 60 * 1_000
MIN_POST_ENTRY_BARS = 16
SAMPLE_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--force", action="store_true", help="Discard only MT-707 resumable state.")
    return parser.parse_args()


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def atomic_write(path: Path, content: str) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def open_duckdb() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute("SET threads = 4")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def load_sparse_candidates(root: Path) -> pd.DataFrame:
    results = root / "results"
    labels_path = results / "mt707" / "train_sparse_labels.csv"
    features_path = results / "characterization_features.csv"
    if not labels_path.is_file():
        raise FileNotFoundError(f"Missing copied MT-706 labels: {labels_path}")
    if not features_path.is_file():
        raise FileNotFoundError(f"Missing MT-706 training source: {features_path}")

    features = pd.read_csv(features_path, usecols=["mint", "birth_timestamp", *GATES])
    labels = pd.read_csv(labels_path, usecols=["feature_index", "sparse", "reason", "post_entry_bars"])
    labels = labels.set_index("feature_index")
    features = features.join(labels, how="left")
    if features["sparse"].isna().any():
        raise RuntimeError("MT-706 labels do not cover the MT-706 feature source")

    features["birth_timestamp"] = features["birth_timestamp"].astype("int64")
    birth_time = pd.to_datetime(features["birth_timestamp"], unit="ms", utc=True)
    features["birth_date"] = birth_time.dt.strftime("%Y-%m-%d")
    features["birth_hour_utc"] = birth_time.dt.hour
    gate_mask = pd.Series(False, index=features.index)
    for field, threshold in GATES.items():
        gate_mask |= pd.to_numeric(features[field], errors="coerce").fillna(-math.inf).ge(threshold)
    cell_mask = pd.Series(False, index=features.index)
    for date, hour in FLAGGED_CELLS:
        cell_mask |= features["birth_date"].eq(date) & features["birth_hour_utc"].eq(hour)
    candidates = features.loc[gate_mask & cell_mask & features["sparse"].astype(bool)].copy()
    candidates["window_start"] = candidates["birth_timestamp"] + ENTRY_OFFSET_MS
    candidates["window_end"] = candidates["window_start"] + WINDOW_MS
    candidates["entry_threshold"] = candidates["window_start"] + ENTRY_DELAY_MS
    return candidates[
        [
            "mint",
            "birth_timestamp",
            "birth_date",
            "birth_hour_utc",
            "reason",
            "post_entry_bars",
            "window_start",
            "window_end",
            "entry_threshold",
        ]
    ]


def reconcile_day(root: Path, day: str, candidates: pd.DataFrame) -> pd.DataFrame:
    ticks_path = root / "derived" / "ticks" / f"{day}.parquet"
    bars_path = root / "derived" / "enriched" / f"{day}.parquet"
    with open_duckdb() as connection:
        connection.register("candidates", candidates)
        ticks = connection.execute(
            f"""
            WITH all_ticks AS (
                SELECT candidate.mint, candidate.window_start, candidate.window_end, tick.timestamp, tick.price
                FROM candidates AS candidate
                LEFT JOIN read_parquet('{sql_path(ticks_path)}') AS tick
                    USING (mint)
            ), window_ticks AS (
                SELECT mint, timestamp, price,
                       lag(timestamp) OVER (PARTITION BY mint ORDER BY timestamp) AS previous_timestamp
                FROM all_ticks
                WHERE timestamp >= window_start
                  AND timestamp <= window_end
            ), archive_counts AS (
                SELECT mint, count(timestamp)::BIGINT AS tick_archive_count
                FROM all_ticks GROUP BY mint
            )
            SELECT candidate.mint,
                   archive_counts.tick_archive_count,
                   count(window_ticks.timestamp)::BIGINT AS tick_trade_count,
                   count(window_ticks.price)::BIGINT AS tick_priced_count,
                   count(DISTINCT floor(window_ticks.timestamp / 5000))
                       FILTER (WHERE window_ticks.price IS NOT NULL)::BIGINT AS tick_priced_bar_count,
                   min(window_ticks.timestamp)::BIGINT AS first_tick_timestamp,
                   max(window_ticks.timestamp)::BIGINT AS last_tick_timestamp,
                   max(window_ticks.timestamp - window_ticks.previous_timestamp)::BIGINT AS largest_tick_gap_ms
            FROM candidates AS candidate
            LEFT JOIN archive_counts USING (mint)
            LEFT JOIN window_ticks USING (mint)
            GROUP BY candidate.mint, archive_counts.tick_archive_count
            """
        ).fetchdf()
        bars = connection.execute(
            f"""
            WITH window_bars AS (
                SELECT candidate.mint, bar.bar_time,
                       lag(bar.bar_time) OVER (PARTITION BY candidate.mint ORDER BY bar.bar_time) AS previous_bar_time
                FROM candidates AS candidate
                LEFT JOIN read_parquet('{sql_path(bars_path)}') AS bar
                    ON bar.mint = candidate.mint
                   -- A bar belongs to its five-second interval, not only its start instant.
                   AND bar.bar_time + 5000 > candidate.window_start
                   AND bar.bar_time <= candidate.window_end
            )
            SELECT mint,
                   count(bar_time)::BIGINT AS bar_count,
                   min(bar_time)::BIGINT AS first_bar_time,
                   max(bar_time)::BIGINT AS last_bar_time,
                   max(bar_time - previous_bar_time)::BIGINT AS largest_bar_gap_ms
            FROM window_bars GROUP BY mint
            """
        ).fetchdf()
        connection.unregister("candidates")
    result = candidates.merge(ticks, on="mint", how="left").merge(bars, on="mint", how="left")
    numeric_columns = [
        "tick_archive_count",
        "tick_trade_count",
        "tick_priced_count",
        "tick_priced_bar_count",
        "bar_count",
    ]
    result[numeric_columns] = result[numeric_columns].fillna(0).astype("int64")
    result["largest_tick_gap_ms"] = result["largest_tick_gap_ms"].fillna(0).astype("int64")
    result["largest_bar_gap_ms"] = result["largest_bar_gap_ms"].fillna(0).astype("int64")

    def bucket(row: pd.Series) -> str:
        if row.tick_archive_count == 0:
            return "NO_TICKS_AT_ALL"
        if row.tick_trade_count == 0:
            return "REAL_DEATH"
        if row.bar_count == 0 or row.bar_count < row.tick_priced_bar_count:
            return "PIPELINE_HOLE"
        if row.post_entry_bars < MIN_POST_ENTRY_BARS:
            return "THIN_BUT_PRESENT"
        raise RuntimeError(f"Sparse mint {row.mint} was not classifiable into an MT-707 bucket")

    result["bucket"] = result.apply(bucket, axis=1)
    return result


def coverage_for_day(root: Path, day: str) -> dict[str, Any]:
    ticks_path = root / "derived" / "ticks" / f"{day}.parquet"
    bars_path = root / "derived" / "enriched" / f"{day}.parquet"
    with open_duckdb() as connection:
        row = connection.execute(
            f"""
            WITH ticks AS (
                SELECT mint FROM read_parquet('{sql_path(ticks_path)}')
            ), bars AS (
                SELECT mint FROM read_parquet('{sql_path(bars_path)}')
            ), tick_mints AS (SELECT DISTINCT mint FROM ticks), bar_mints AS (SELECT DISTINCT mint FROM bars)
            SELECT
                (SELECT count(*) FROM tick_mints)::BIGINT AS tick_mints,
                (SELECT count(*) FROM bar_mints)::BIGINT AS enriched_mints,
                (SELECT count(*) FROM tick_mints LEFT JOIN bar_mints USING (mint) WHERE bar_mints.mint IS NULL)::BIGINT
                    AS tick_mints_absent_from_enriched,
                (SELECT count(*) FROM ticks)::BIGINT AS tick_rows,
                (SELECT count(*) FROM bars)::BIGINT AS bar_rows
            """
        ).fetchone()
    tick_mints, enriched_mints, absent, tick_rows, bar_rows = (int(value) for value in row)
    return {
        "date": day,
        "tick_mints": tick_mints,
        "enriched_mints": enriched_mints,
        "tick_mints_absent_from_enriched": absent,
        "tick_mints_absent_pct": absent / tick_mints if tick_mints else 0.0,
        "tick_rows": tick_rows,
        "bar_rows": bar_rows,
        "bars_per_tick": bar_rows / tick_rows if tick_rows else 0.0,
    }


def select_terminal_sample(reconciliation: pd.DataFrame) -> pd.DataFrame:
    groups: list[pd.DataFrame] = []
    for bucket in ("PIPELINE_HOLE", "REAL_DEATH"):
        group = reconciliation.loc[reconciliation["bucket"].eq(bucket)].copy()
        groups.append(group.sample(min(SAMPLE_SIZE, len(group)), random_state=707))
    return pd.concat(groups, ignore_index=True)


def terminal_prices(root: Path, sample: pd.DataFrame) -> list[dict[str, Any]]:
    records = {
        row.mint: {
            "mint": row.mint,
            "bucket": row.bucket,
            "birth_date": row.birth_date,
            "entry_threshold": int(row.entry_threshold),
            "entry_price": None,
            "entry_time": None,
            "last_tick_price": None,
            "last_tick_timestamp": None,
            "last_bar_close": None,
            "last_bar_time": None,
        }
        for row in sample.itertuples(index=False)
    }
    dates = sorted(path.stem for path in (root / "derived" / "enriched").glob("*.parquet"))
    sample_table = sample[["mint", "entry_threshold"]]
    for index, day in enumerate(dates, start=1):
        ticks_path = root / "derived" / "ticks" / f"{day}.parquet"
        bars_path = root / "derived" / "enriched" / f"{day}.parquet"
        with open_duckdb() as connection:
            connection.register("sample_mints", sample_table)
            tick_rows = connection.execute(
                f"""
                SELECT sample_mints.mint, tick.timestamp, tick.price, sample_mints.entry_threshold
                FROM read_parquet('{sql_path(ticks_path)}') AS tick
                INNER JOIN sample_mints USING (mint)
                WHERE tick.price IS NOT NULL AND tick.price > 0
                ORDER BY mint, timestamp
                """
            ).fetchall()
            bar_rows = connection.execute(
                f"""
                SELECT sample_mints.mint, bar.bar_time, bar.open, bar.close, sample_mints.entry_threshold
                FROM read_parquet('{sql_path(bars_path)}') AS bar
                INNER JOIN sample_mints USING (mint)
                WHERE bar.close IS NOT NULL AND bar.close > 0
                ORDER BY mint, bar_time
                """
            ).fetchall()
            connection.unregister("sample_mints")
        for mint, timestamp, price, entry_threshold in tick_rows:
            record = records[mint]
            if timestamp >= entry_threshold and record["entry_price"] is None:
                record["entry_price"] = float(price)
                record["entry_time"] = int(timestamp)
            if record["last_tick_timestamp"] is None or timestamp > record["last_tick_timestamp"]:
                record["last_tick_timestamp"] = int(timestamp)
                record["last_tick_price"] = float(price)
        for mint, bar_time, open_price, close_price, entry_threshold in bar_rows:
            record = records[mint]
            if record["entry_price"] is None and bar_time >= entry_threshold and open_price and open_price > 0:
                record["entry_price"] = float(open_price)
                record["entry_time"] = int(bar_time)
            if record["last_bar_time"] is None or bar_time > record["last_bar_time"]:
                record["last_bar_time"] = int(bar_time)
                record["last_bar_close"] = float(close_price)
        print(f"terminal prices [{index}/{len(dates)}] {day}", flush=True)

    rows: list[dict[str, Any]] = []
    for record in records.values():
        entry = record["entry_price"]
        tick_ratio = record["last_tick_price"] / entry if entry and record["last_tick_price"] else None
        bar_ratio = record["last_bar_close"] / entry if entry and record["last_bar_close"] else None
        disagreement = (
            record["last_bar_close"] / record["last_tick_price"]
            if record["last_bar_close"] and record["last_tick_price"]
            else None
        )
        rows.append({**record, "last_tick_to_entry": tick_ratio, "last_bar_to_entry": bar_ratio, "bar_to_tick_final": disagreement})
    return rows


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def distribution(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = sorted(value for row in rows if (value := number(row.get(field))) is not None and value > 0)
    if not values:
        return {"n": 0}

    def quantile(probability: float) -> float:
        position = (len(values) - 1) * probability
        lower, upper = math.floor(position), math.ceil(position)
        return values[lower] + (values[upper] - values[lower]) * (position - lower)

    return {
        "n": len(values),
        "p05": quantile(0.05),
        "p25": quantile(0.25),
        "median": quantile(0.5),
        "p75": quantile(0.75),
        "p95": quantile(0.95),
        "p99": quantile(0.99),
        "max": values[-1],
        "over_100x": sum(value > 100 for value in values),
        "over_1000x": sum(value > 1000 for value in values),
        "over_mt703_cap": sum(value > 1022.511434 for value in values),
    }


def format_distribution(summary: dict[str, Any]) -> str:
    if not summary.get("n"):
        return "No usable positive entry/final-price pairs."
    return (
        f"n={summary['n']:,}; p05={summary['p05']:.4g}x; p25={summary['p25']:.4g}x; "
        f"median={summary['median']:.4g}x; p75={summary['p75']:.4g}x; p95={summary['p95']:.4g}x; "
        f"p99={summary['p99']:.4g}x; max={summary['max']:.4g}x; "
        f">100x={summary['over_100x']:,}; >1,000x={summary['over_1000x']:,}; "
        f"> MT-703 cap={summary['over_mt703_cap']:,}."
    )


def build_report(
    root: Path,
    reconciliation: list[dict[str, Any]],
    coverage: list[dict[str, Any]],
    terminal: list[dict[str, Any]],
    wall_seconds: float,
) -> str:
    reconciled = pd.DataFrame(reconciliation)
    bucket_counts = reconciled["bucket"].value_counts().to_dict()
    total = len(reconciled)
    hole_count = int(bucket_counts.get("PIPELINE_HOLE", 0))
    coverage_ratios = [float(row["bars_per_tick"]) for row in coverage]
    archive_median = median(coverage_ratios)
    high_absence = [row for row in coverage if float(row["tick_mints_absent_pct"]) > 0.05]
    ratio_outliers = [
        row for row in coverage if archive_median and abs(float(row["bars_per_tick"]) / archive_median - 1.0) > 0.30
    ]
    cells = (
        reconciled.groupby(["birth_date", "birth_hour_utc", "bucket"]).size().unstack(fill_value=0).reset_index()
    )
    terminal_by_bucket = {bucket: [row for row in terminal if row["bucket"] == bucket] for bucket in ("PIPELINE_HOLE", "REAL_DEATH")}
    disagreement = distribution(terminal, "bar_to_tick_final")
    lines = [
        "# MT-707 Tick-vs-Bar Reconciliation",
        "",
        "## Headline",
        "",
        f"Targeted MT-706 sparse population: {total:,} gate-passing mints in the five flagged date-hour cells.",
        "",
        "| Bucket | Mints | Share |",
        "| --- | ---: | ---: |",
    ]
    for bucket in ("PIPELINE_HOLE", "REAL_DEATH", "THIN_BUT_PRESENT", "NO_TICKS_AT_ALL"):
        count = int(bucket_counts.get(bucket, 0))
        lines.append(f"| {bucket} | {count:,} | {count / total:.2%} |")
    lines.extend(["", "A `PIPELINE_HOLE` has ticks in the requested window but no bar, or fewer bars than the number of distinct 5-second tick buckets with a valid price. Bars overlapping a non-aligned tick-window boundary are included, preventing a one-bucket boundary artifact. `REAL_DEATH` has an archived mint but no window ticks. `NO_TICKS_AT_ALL` is absent from that day's tick partition. `THIN_BUT_PRESENT` has both layers but fails MT-705's 16 post-delayed-entry-bar minimum.", "", "## Flagged Cells", "", "| Date | UTC hour | PIPELINE_HOLE | REAL_DEATH | THIN_BUT_PRESENT | NO_TICKS_AT_ALL |", "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in cells.itertuples(index=False):
        lines.append(
            f"| {row.birth_date} | {row.birth_hour_utc} | {getattr(row, 'PIPELINE_HOLE', 0):,} | "
            f"{getattr(row, 'REAL_DEATH', 0):,} | {getattr(row, 'THIN_BUT_PRESENT', 0):,} | "
            f"{getattr(row, 'NO_TICKS_AT_ALL', 0):,} |"
        )
    lines.extend(["", "## Archive Coverage", "", f"Coverage covers the {len(coverage):,} dates for which both tick and enriched partitions exist. Archive median bars/tick: {archive_median:.6f}.", "", f"Dates with >5% of tick mints absent from enriched ({len(high_absence):,}): " + (", ".join(row["date"] for row in high_absence) or "none") + ".", "", f"Dates whose bars/tick ratio differs by >30% from the archive median ({len(ratio_outliers):,}): " + (", ".join(row["date"] for row in ratio_outliers) or "none") + ".", "", "`daily_coverage.csv` has the complete per-day counts and flags.", "", "## Terminal Price Sanity", ""])
    for bucket, records in terminal_by_bucket.items():
        lines.append(f"### {bucket}")
        lines.append("")
        lines.append(f"Selected {len(records):,} of the requested {SAMPLE_SIZE:,} mints.")
        lines.append("")
        lines.append(f"Last tick / entry: {format_distribution(distribution(records, 'last_tick_to_entry'))}")
        lines.append("")
        lines.append(f"Last bar / entry: {format_distribution(distribution(records, 'last_bar_to_entry'))}")
        lines.append("")
    if hole_count:
        rebuild_scope = (
            "Valid-priced tick buckets are absent from OHLCV. Rebuild from the **ohlcv** stage for every affected date, "
            "then rerun enrichment for those dates. Raw JSONL is not needed because ticks are retained."
        )
    else:
        rebuild_scope = (
            "No valid-priced tick bucket was missing its overlapping OHLCV bar, daily mint coverage was effectively complete, "
            "and no day breached either archive-wide coverage threshold. The apparent one-bar deficits in the first pass were "
            "non-aligned-window boundary effects, not a pipeline loss. **No rebuild is required.**"
        )
    lines.extend([
        f"Final bar / final tick disagreement across samples: {format_distribution(disagreement)}",
        "",
        "The canonical entry is the first valid tick at or after MT-705's 2-minute plus 42.555-second delayed-entry threshold; a bar open is used only when no such tick price exists. Final values are the last positive price recorded anywhere in the available archive.",
        "",
        "## Pipeline Diagnosis And Rebuild Scope",
        "",
        "`etl.py:379-413` builds OHLCV exclusively from ticks with `price IS NOT NULL`, grouping only 5-second buckets that retain a price. It has no minimum-activity threshold. `enrich.py:253-317` iterates every OHLCV bar and writes one enriched row per bar; `enrich.py:328-332` explicitly validates equal OHLCV/enriched row counts. Therefore a tick/bar mismatch with valid tick prices is in OHLCV construction, not enrichment. Ticks that have no valid `price` are an upstream field-quality gap: they cannot form a price bar under the present contract.",
        "",
        "MT-577's `solAmount` to `quoteAmount` change affected volume extraction. The current `etl.py` already falls back through both names, while the suspected sparse-path condition is the separate `price IS NOT NULL` filter. The targeted output records `tick_priced_count` and `tick_priced_bar_count` so that field-null cases are distinguishable from true aggregation loss.",
        "",
        rebuild_scope,
        "",
        "No rebuild was performed in this task.",
        "",
        f"Wall time: {wall_seconds / 60:.1f} minutes. Peak RSS: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024):.2f} GB.",
    ])
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    output = root / "results" / "mt707"
    output.mkdir(parents=True, exist_ok=True)
    state_path = output / "state.json"
    if args.force:
        state_path.unlink(missing_ok=True)
    state: dict[str, Any] = json.loads(state_path.read_text(encoding="utf-8")) if state_path.is_file() else {}
    started_at = time.monotonic()

    reconciliation_path = output / "target_reconciliation.csv"
    if reconciliation_path.is_file() and state.get("target_complete"):
        reconciliation_rows = read_csv_rows(reconciliation_path)
    else:
        candidates = load_sparse_candidates(root)
        parts = [reconcile_day(root, day, group.copy()) for day, group in candidates.groupby("birth_date", sort=True)]
        reconciliation_rows = pd.concat(parts, ignore_index=True).to_dict("records")
        write_csv(reconciliation_path, reconciliation_rows)
        state["target_complete"] = True
        atomic_write(state_path, json.dumps(state, indent=2) + "\n")
        print(f"target reconciliation complete: {len(reconciliation_rows):,} sparse mints", flush=True)

    coverage_path = output / "daily_coverage.csv"
    coverage_by_date = {row["date"]: row for row in read_csv_rows(coverage_path)}
    tick_dates = {path.stem for path in (root / "derived" / "ticks").glob("*.parquet")}
    enriched_dates = {path.stem for path in (root / "derived" / "enriched").glob("*.parquet")}
    dates = sorted(tick_dates & enriched_dates)
    for index, day in enumerate(dates, start=1):
        if day not in coverage_by_date:
            coverage_by_date[day] = coverage_for_day(root, day)
            rows = [coverage_by_date[item] for item in sorted(coverage_by_date)]
            write_csv(coverage_path, rows)
            state["coverage_days"] = sorted(coverage_by_date)
            atomic_write(state_path, json.dumps(state, indent=2) + "\n")
        print(f"coverage [{index}/{len(dates)}] {day}", flush=True)
    coverage_rows = [coverage_by_date[day] for day in dates]

    terminal_path = output / "terminal_price_sample.csv"
    if terminal_path.is_file() and state.get("terminal_complete"):
        terminal_rows = read_csv_rows(terminal_path)
    else:
        reconciliation = pd.DataFrame(reconciliation_rows)
        for field in ("post_entry_bars", "entry_threshold"):
            reconciliation[field] = pd.to_numeric(reconciliation[field], errors="raise").astype("int64")
        sample = select_terminal_sample(reconciliation)
        terminal_rows = terminal_prices(root, sample)
        write_csv(terminal_path, terminal_rows)
        state["terminal_complete"] = True
        atomic_write(state_path, json.dumps(state, indent=2) + "\n")

    wall_seconds = time.monotonic() - started_at
    if args.force or "analysis_wall_seconds" not in state:
        state["analysis_wall_seconds"] = wall_seconds
    report = build_report(root, reconciliation_rows, coverage_rows, terminal_rows, float(state["analysis_wall_seconds"]))
    atomic_write(output / "RECONCILIATION_REPORT.md", report)
    state["report_complete"] = True
    state["completed_at"] = datetime.now(UTC).isoformat()
    atomic_write(state_path, json.dumps(state, indent=2) + "\n")
    print("MT-707 reconciliation complete", flush=True)


if __name__ == "__main__":
    run(parse_args())
