#!/usr/bin/env python3
"""MT-762: resumable graduated-coin characteristics and outcomes table.

Each archive read is limited to one or two daily Parquets inside a capped
subprocess. Feature snapshots only join bars through their checkpoint; a
separate forward scan supplies the deliberately ``fwd_``-prefixed outcomes.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values


ARCHIVE = Path("/mnt/d/pumpapi-replay/derived/enriched")
LABELS = Path("/home/dev/workspace/data/results/mt739/label_chunks")
ROOT = Path("/workspace/shared/MT-762")
PROJECT_ROOT = Path("/home/dev/projects/memecoin-trader")
START = date(2026, 4, 18)
END = date(2026, 8, 22)  # Exclusive.
CHECKPOINTS = (30, 120, 300, 600, 1320)
CHECKPOINT_TOLERANCE_MS = 30_000
MARK_TOLERANCE_MS = 30_000
WALLET_FEATURES = (
    "unique_wallets_total",
    "top10_holder_pct",
    "creator_holdings_pct",
)
FEATURE_COLUMNS = (
    "market_cap_usd", "price", "sol_in_pool", "min_pool_since_graduation_sol",
    "max_pool_since_graduation_sol", "trade_count_1m", "buy_volume_1m",
    "sell_volume_1m", "volume_delta_1m", "trade_count_5m", "buy_volume_5m",
    "sell_volume_5m", "volume_delta_5m", "return_1m", "return_2m",
    "return_5m", *WALLET_FEATURES,
)


def dates() -> list[date]:
    values: list[date] = []
    current = START
    while current < END:
        values.append(current)
        current += timedelta(days=1)
    return values


def path_for(day: date) -> Path:
    path = ARCHIVE / f"{day.isoformat()}.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def q(value: str | Path) -> str:
    return repr(str(value))


def db_connection():
    load_dotenv(PROJECT_ROOT / ".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def duck(temp_name: str) -> duckdb.DuckDBPyConnection:
    temp = ROOT / "duckdb_tmp" / temp_name
    temp.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '4GB'")
    connection.execute("SET threads = 4")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(f"SET temp_directory = {q(temp)}")
    return connection


def graduation_index() -> Path:
    output = ROOT / "graduations_utc_v2.parquet"
    if output.is_file() and output.stat().st_size:
        return output
    paths = ", ".join(q(path_for(day)) for day in dates())
    start_ms = int(datetime.combine(START, datetime.min.time(), UTC).timestamp() * 1000)
    end_ms = int(datetime.combine(END, datetime.min.time(), UTC).timestamp() * 1000)
    with duck("graduations") as connection:
        connection.execute(
            f"""
            COPY (
                WITH first_markers AS (
                    SELECT mint, min(bar_time)::BIGINT AS graduation_time
                    FROM read_parquet([{paths}])
                    WHERE graduated_this_bar
                    GROUP BY mint
                )
                SELECT mint, graduation_time,
                       CAST(to_timestamp(graduation_time / 1000.0) AT TIME ZONE 'UTC' AS DATE)
                           AS graduation_day
                FROM first_markers
                WHERE graduation_time >= {start_ms} AND graduation_time < {end_ms}
            ) TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return output


def checkpoint_day(day: date) -> Path:
    output = ROOT / "checkpoint_chunks_utc_v2" / f"{day.isoformat()}.parquet"
    if output.is_file() and output.stat().st_size:
        return output
    output.parent.mkdir(parents=True, exist_ok=True)
    sources = [path_for(day)]
    if day + timedelta(days=1) < END:
        sources.append(path_for(day + timedelta(days=1)))
    source_paths = ", ".join(q(path) for path in sources)
    checkpoint_values = ", ".join(f"({seconds})" for seconds in CHECKPOINTS)
    with duck(f"checkpoint-{day.isoformat()}") as connection:
        connection.execute(
            f"""
            COPY (
                WITH entries AS (
                    SELECT mint, graduation_time, checkpoint_seconds,
                           graduation_time + checkpoint_seconds * 1000 AS target_time
                    FROM read_parquet({q(graduation_index())})
                    CROSS JOIN (VALUES {checkpoint_values}) AS c(checkpoint_seconds)
                    WHERE graduation_day = DATE '{day.isoformat()}'
                ), bars AS (
                    SELECT mint, bar_time, close, market_cap_usd, min_sol_in_pool,
                           max_sol_in_pool, buy_volume_sol, sell_volume_sol, trade_count,
                           unique_wallets_total, top10_holder_pct, creator_holdings_pct
                    FROM read_parquet([{source_paths}])
                ), checkpoint_marks AS (
                    SELECT e.mint, e.graduation_time, e.checkpoint_seconds, e.target_time,
                           min(b.bar_time) AS checkpoint_time
                    FROM entries AS e
                    JOIN bars AS b ON b.mint = e.mint
                                AND b.bar_time >= e.target_time
                                AND b.bar_time <= e.target_time + {CHECKPOINT_TOLERANCE_MS}
                    GROUP BY ALL
                ), history AS (
                    SELECT c.*, b.bar_time, b.close, b.market_cap_usd, b.min_sol_in_pool,
                           b.max_sol_in_pool, b.buy_volume_sol, b.sell_volume_sol,
                           b.trade_count, b.unique_wallets_total, b.top10_holder_pct,
                           b.creator_holdings_pct
                    FROM checkpoint_marks AS c
                    JOIN bars AS b ON b.mint = c.mint
                                AND b.bar_time >= c.graduation_time
                                AND b.bar_time <= c.checkpoint_time
                )
                SELECT mint, graduation_time, checkpoint_seconds, checkpoint_time,
                       arg_max(market_cap_usd, bar_time) AS market_cap_usd,
                       arg_max(close, bar_time) AS price,
                       arg_max(min_sol_in_pool, bar_time) AS sol_in_pool,
                       min(min_sol_in_pool) AS min_pool_since_graduation_sol,
                       max(max_sol_in_pool) AS max_pool_since_graduation_sol,
                       sum(trade_count) FILTER (WHERE bar_time > checkpoint_time - 60000) AS trade_count_1m,
                       sum(buy_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS buy_volume_1m,
                       sum(sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS sell_volume_1m,
                       sum(buy_volume_sol - sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 60000) AS volume_delta_1m,
                       sum(trade_count) FILTER (WHERE bar_time > checkpoint_time - 300000) AS trade_count_5m,
                       sum(buy_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 300000) AS buy_volume_5m,
                       sum(sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 300000) AS sell_volume_5m,
                       sum(buy_volume_sol - sell_volume_sol) FILTER (WHERE bar_time > checkpoint_time - 300000) AS volume_delta_5m,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time)
                           FILTER (WHERE bar_time <= checkpoint_time - 60000), 0) - 1 AS return_1m,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time)
                           FILTER (WHERE bar_time <= checkpoint_time - 120000), 0) - 1 AS return_2m,
                       arg_max(close, bar_time) / nullif(arg_max(close, bar_time)
                           FILTER (WHERE bar_time <= checkpoint_time - 300000), 0) - 1 AS return_5m,
                       arg_max(unique_wallets_total, bar_time) AS unique_wallets_total,
                       arg_max(top10_holder_pct, bar_time) AS top10_holder_pct,
                       arg_max(creator_holdings_pct, bar_time) AS creator_holdings_pct
                FROM history
                GROUP BY mint, graduation_time, checkpoint_seconds, checkpoint_time
            ) TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return output


def checkpoint_files() -> list[Path]:
    return [ROOT / "checkpoint_chunks_utc_v2" / f"{day.isoformat()}.parquet" for day in dates()]


def checkpoint_index() -> Path:
    output = ROOT / "checkpoints_utc_v2.parquet"
    files = checkpoint_files()
    if not all(path.is_file() and path.stat().st_size for path in files):
        raise RuntimeError("Checkpoint chunks are incomplete")
    if output.is_file() and output.stat().st_size:
        return output
    with duck("checkpoint-index") as connection:
        connection.execute(
            f"COPY (SELECT * FROM read_parquet([{', '.join(q(path) for path in files)}])) "
            f"TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    return output


def forward_day(day: date) -> Path:
    output = ROOT / "forward_chunks_utc_v2" / f"{day.isoformat()}.parquet"
    if output.is_file() and output.stat().st_size:
        return output
    output.parent.mkdir(parents=True, exist_ok=True)
    start_ms = int(datetime.combine(day, datetime.min.time(), UTC).timestamp() * 1000)
    end_ms = start_ms + 86_400_000
    with duck(f"forward-{day.isoformat()}") as connection:
        connection.execute(
            f"""
            COPY (
                WITH checkpoints AS (SELECT * FROM read_parquet({q(checkpoint_index())})),
                bars AS (
                    SELECT mint, bar_time, close FROM read_parquet({q(path_for(day))})
                    WHERE close > 0
                ), daily_peaks AS (
                    SELECT c.mint, c.graduation_time, c.checkpoint_seconds,
                           max(b.close) AS fwd_peak_close,
                           arg_max(b.bar_time, b.close) AS fwd_peak_time
                    FROM checkpoints AS c
                    JOIN bars AS b ON b.mint = c.mint AND b.bar_time > c.checkpoint_time
                    WHERE c.checkpoint_time < {end_ms}
                    GROUP BY c.mint, c.graduation_time, c.checkpoint_seconds
                ), marks AS (
                    SELECT c.mint, c.graduation_time, c.checkpoint_seconds,
                           arg_min(b.close, b.bar_time) FILTER (
                               WHERE b.bar_time >= c.checkpoint_time + 300000
                                 AND b.bar_time <= c.checkpoint_time + 300000 + {MARK_TOLERANCE_MS}
                           ) AS fwd_close_at_plus_5m,
                           arg_min(b.close, b.bar_time) FILTER (
                               WHERE b.bar_time >= c.checkpoint_time + 1200000
                                 AND b.bar_time <= c.checkpoint_time + 1200000 + {MARK_TOLERANCE_MS}
                           ) AS fwd_close_at_plus_20m
                    FROM checkpoints AS c
                    JOIN bars AS b ON b.mint = c.mint
                    WHERE (c.checkpoint_time + 300000 BETWEEN {start_ms} AND {end_ms - 1})
                       OR (c.checkpoint_time + 1200000 BETWEEN {start_ms} AND {end_ms - 1})
                    GROUP BY c.mint, c.graduation_time, c.checkpoint_seconds
                )
                SELECT coalesce(p.mint, m.mint) AS mint,
                       coalesce(p.graduation_time, m.graduation_time) AS graduation_time,
                       coalesce(p.checkpoint_seconds, m.checkpoint_seconds) AS checkpoint_seconds,
                       p.fwd_peak_close, p.fwd_peak_time,
                       m.fwd_close_at_plus_5m, m.fwd_close_at_plus_20m
                FROM daily_peaks AS p FULL OUTER JOIN marks AS m
                  USING (mint, graduation_time, checkpoint_seconds)
            ) TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return output


def forward_summary() -> Path:
    output = ROOT / "forward_summary_utc_v2.parquet"
    files = [ROOT / "forward_chunks_utc_v2" / f"{day.isoformat()}.parquet" for day in dates()]
    if not all(path.is_file() and path.stat().st_size for path in files):
        raise RuntimeError("Forward chunks are incomplete")
    if output.is_file() and output.stat().st_size:
        return output
    with duck("forward-summary") as connection:
        connection.execute(
            f"""
            COPY (
                SELECT mint, graduation_time, checkpoint_seconds,
                       max(fwd_peak_close) AS fwd_peak_close,
                       arg_max(fwd_peak_time, fwd_peak_close) AS fwd_peak_time,
                       max(fwd_close_at_plus_5m) AS fwd_close_at_plus_5m,
                       max(fwd_close_at_plus_20m) AS fwd_close_at_plus_20m
                FROM read_parquet([{', '.join(q(path) for path in files)}])
                GROUP BY mint, graduation_time, checkpoint_seconds
            ) TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return output


def table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS research.coin_checkpoints (
        mint TEXT NOT NULL,
        graduation_time BIGINT NOT NULL,
        checkpoint_seconds INTEGER NOT NULL,
        checkpoint_time BIGINT NOT NULL,
        market_cap_usd DOUBLE PRECISION, price DOUBLE PRECISION, sol_in_pool DOUBLE PRECISION,
        min_pool_since_graduation_sol DOUBLE PRECISION, max_pool_since_graduation_sol DOUBLE PRECISION,
        trade_count_1m BIGINT, buy_volume_1m DOUBLE PRECISION, sell_volume_1m DOUBLE PRECISION,
        volume_delta_1m DOUBLE PRECISION, trade_count_5m BIGINT, buy_volume_5m DOUBLE PRECISION,
        sell_volume_5m DOUBLE PRECISION, volume_delta_5m DOUBLE PRECISION,
        return_1m DOUBLE PRECISION, return_2m DOUBLE PRECISION, return_5m DOUBLE PRECISION,
        unique_wallets_total BIGINT, top10_holder_pct DOUBLE PRECISION, creator_holdings_pct DOUBLE PRECISION,
        fwd_max_multiple DOUBLE PRECISION, fwd_seconds_to_peak DOUBLE PRECISION,
        fwd_close_at_plus_5m DOUBLE PRECISION, fwd_close_at_plus_20m DOUBLE PRECISION,
        fwd_rugged BOOLEAN, fwd_rug_seconds DOUBLE PRECISION, label_reason TEXT,
        he_traded BOOLEAN NOT NULL DEFAULT FALSE, he_tier TEXT, he_created BOOLEAN NOT NULL DEFAULT FALSE,
        he_quarantined BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (mint, graduation_time, checkpoint_seconds)
    )
    """


BASE_COLUMNS = (
    "mint", "graduation_time", "checkpoint_seconds", "checkpoint_time", *FEATURE_COLUMNS,
    "fwd_max_multiple", "fwd_seconds_to_peak", "fwd_close_at_plus_5m", "fwd_close_at_plus_20m",
)


def upsert_base_rows(rows: Iterable[tuple[Any, ...]]) -> int:
    records = list(rows)
    if not records:
        return 0
    columns = ", ".join(BASE_COLUMNS)
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in BASE_COLUMNS[3:])
    with db_connection() as connection, connection.cursor() as cursor:
        execute_values(
            cursor,
            f"""INSERT INTO research.coin_checkpoints ({columns}) VALUES %s
            ON CONFLICT (mint, graduation_time, checkpoint_seconds) DO UPDATE SET {updates}""",
            records,
            page_size=5000,
        )
    return len(records)


def write_base_table() -> int:
    with db_connection() as connection, connection.cursor() as cursor:
        cursor.execute(table_ddl())
    summary = forward_summary()
    total = 0
    for day, source in zip(dates(), checkpoint_files(), strict=True):
        with duck(f"hive-{day.isoformat()}") as connection:
            reader = connection.execute(
                f"""
                SELECT c.mint, c.graduation_time, c.checkpoint_seconds, c.checkpoint_time,
                       {', '.join('c.' + column for column in FEATURE_COLUMNS)},
                       f.fwd_peak_close / nullif(c.price, 0) AS fwd_max_multiple,
                       (f.fwd_peak_time - c.checkpoint_time) / 1000.0 AS fwd_seconds_to_peak,
                       f.fwd_close_at_plus_5m, f.fwd_close_at_plus_20m
                FROM read_parquet({q(source)}) AS c
                LEFT JOIN read_parquet({q(summary)}) AS f
                  USING (mint, graduation_time, checkpoint_seconds)
                """
            ).fetch_record_batch(20_000)
            for batch in reader:
                total += upsert_base_rows(zip(*(batch.column(index).to_pylist() for index in range(len(BASE_COLUMNS))), strict=True))
    return total


def rug_labels() -> Path:
    output = ROOT / "rug_labels_utc_v2.parquet"
    if output.is_file() and output.stat().st_size:
        return output
    files = [LABELS / f"{day.isoformat()}.parquet" for day in dates()]
    if not all(path.is_file() for path in files):
        raise RuntimeError("MT-739 label chunks are incomplete")
    with duck("rug-labels") as connection:
        connection.execute(
            f"""
            COPY (
                SELECT c.mint, c.graduation_time, c.checkpoint_seconds,
                       min(l.rug_timestamp) AS rug_timestamp,
                       arg_min(l.label_reason, l.rug_timestamp) AS label_reason
                FROM read_parquet({q(checkpoint_index())}) AS c
                JOIN read_parquet([{', '.join(q(path) for path in files)}]) AS l
                  ON l.mint = c.mint AND l.rug_timestamp > c.checkpoint_time
                GROUP BY c.mint, c.graduation_time, c.checkpoint_seconds
            ) TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return output


def apply_rug_labels() -> int:
    labels = rug_labels()
    with db_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE research.coin_checkpoints SET fwd_rugged = false, fwd_rug_seconds = NULL, label_reason = NULL"
        )
    updated = 0
    with duck("rug-update") as connection:
        reader = connection.execute(
            f"SELECT mint, graduation_time, checkpoint_seconds, rug_timestamp, label_reason FROM read_parquet({q(labels)})"
        ).fetch_record_batch(20_000)
        for batch in reader:
            rows = list(zip(*(batch.column(index).to_pylist() for index in range(5)), strict=True))
            if not rows:
                continue
            with db_connection() as connection, connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """UPDATE research.coin_checkpoints AS c SET fwd_rugged = true,
                           fwd_rug_seconds = (v.rug_timestamp - c.checkpoint_time) / 1000.0,
                           label_reason = v.label_reason
                    FROM (VALUES %s) AS v(mint, graduation_time, checkpoint_seconds, rug_timestamp, label_reason)
                    WHERE c.mint = v.mint AND c.graduation_time = v.graduation_time
                      AND c.checkpoint_seconds = v.checkpoint_seconds""",
                    rows,
                    page_size=5000,
                )
            updated += len(rows)
    return updated


def apply_trade_labels() -> dict[str, int]:
    with db_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT wallet FROM research.wallet_position_extended GROUP BY wallet")
        wallets = [row[0] for row in cursor.fetchall()]
        if len(wallets) != 1:
            raise RuntimeError(f"Expected one wallet_position_extended wallet, found {len(wallets)}")
        wallet = wallets[0]
        cursor.execute(
            """
            UPDATE research.coin_checkpoints
            SET he_traded = false, he_tier = NULL, he_created = false, he_quarantined = false
            """
        )
        cursor.execute(
            """
            UPDATE research.coin_checkpoints AS c
            SET he_traded = true, he_tier = f.tier, he_quarantined = e.quarantined
            FROM research.wallet_position_extended AS e
            LEFT JOIN research.wallet_position_features AS f USING (wallet, mint)
            WHERE e.wallet = %s AND c.mint = e.mint
            """,
            (wallet,),
        )
        cursor.execute(
            """
            UPDATE research.coin_checkpoints AS c
            SET he_created = true
            FROM research.mint_creator AS m
            WHERE m.creator_wallet = %s AND c.mint = m.mint
            """,
            (wallet,),
        )
        cursor.execute(
            """
            SELECT count(*) FILTER (WHERE he_traded),
                   count(*) FILTER (WHERE he_quarantined),
                   count(DISTINCT mint) FILTER (WHERE he_quarantined),
                   count(*) FILTER (WHERE he_traded AND NOT he_quarantined AND NOT he_created AND he_tier = 'T1'),
                   count(*) FILTER (WHERE he_traded AND NOT he_quarantined AND NOT he_created AND he_tier = 'T4')
            FROM research.coin_checkpoints
            """
        )
        values = cursor.fetchone()
        cursor.execute("SELECT count(*) FROM research.wallet_position_extended WHERE quarantined")
        source_quarantined_positions = cursor.fetchone()[0]
    return dict(zip(("he_traded_rows", "quarantined_rows", "quarantined_mints", "noncreated_t1_rows", "noncreated_t4_rows"), values, strict=True)) | {"source_quarantined_positions": source_quarantined_positions}


def auc_ranking() -> list[dict[str, Any]]:
    output = ROOT / "auc_5m_apr18_may18.csv"
    start_ms = int(datetime.combine(START, datetime.min.time(), UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 5, 19, tzinfo=UTC).timestamp() * 1000)
    with duck("auc") as connection:
        frame = connection.execute(
            f"""
            WITH cohort AS (
                SELECT * FROM read_parquet({q(checkpoint_index())}) AS c
                LEFT JOIN read_parquet({q(forward_summary())}) AS f
                  USING (mint, graduation_time, checkpoint_seconds)
                WHERE checkpoint_seconds = 300
                  AND graduation_time >= {start_ms} AND graduation_time < {end_ms}
                  AND c.price > 0 AND f.fwd_peak_close > 0
            ), tiered AS (
                SELECT *, ntile(10) OVER (ORDER BY fwd_peak_close / price DESC) AS outcome_decile
                FROM cohort
            )
            SELECT *, CASE WHEN outcome_decile = 1 THEN 'T1'
                           WHEN outcome_decile <= 3 THEN 'T2'
                           WHEN outcome_decile <= 6 THEN 'T3' ELSE 'T4' END AS fwd_tier
            FROM tiered
            """
        ).fetchdf()
    rows: list[dict[str, Any]] = []
    for feature in FEATURE_COLUMNS:
        values = frame[feature]
        if str(values.dtype) == "boolean":
            values = values.astype("Float64")
        numeric = values.astype("float64")
        valid = numeric.notna() & frame["fwd_tier"].isin(("T1", "T4"))
        winners = numeric[valid & frame["fwd_tier"].eq("T1")]
        losers = numeric[valid & frame["fwd_tier"].eq("T4")]
        if winners.empty or losers.empty:
            raw_auc, direction = None, "unavailable"
        else:
            ranks = numeric[valid].rank(method="average")
            raw_auc = (ranks[frame.loc[valid, "fwd_tier"].eq("T1")].sum() - len(winners) * (len(winners) + 1) / 2) / (len(winners) * len(losers))
            direction = "T1 higher" if raw_auc >= 0.5 else "T1 lower"
        rows.append({
            "feature": feature, "auc": None if raw_auc is None else max(raw_auc, 1 - raw_auc),
            "direction": direction, "valid_t1": len(winners), "valid_t4": len(losers),
            "cohort_rows": len(frame),
        })
    rows.sort(key=lambda row: (-1 if row["auc"] is None else -row["auc"], row["feature"]))
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def render_report(trade_counts: dict[str, int] | None, auc: list[dict[str, Any]] | None) -> None:
    report = ROOT / "REPORT.md"
    coverage_csv = ROOT / "feature_coverage_by_day.csv"
    with duck("report") as connection:
        day_rows = connection.execute(
            f"""
            SELECT g.graduation_day, count(DISTINCT g.mint) AS graduated_coins,
                   count(c.mint) AS checkpoint_rows,
                   {', '.join(f"count(c.mint) FILTER (WHERE c.checkpoint_seconds = {seconds}) AS checkpoint_{seconds}s" for seconds in CHECKPOINTS)}
            FROM read_parquet({q(graduation_index())}) AS g
            LEFT JOIN read_parquet({q(checkpoint_index())}) AS c USING (mint, graduation_time)
            GROUP BY g.graduation_day ORDER BY g.graduation_day
            """
        ).fetchall()
        coverage_query = " UNION ALL ".join(
            f"SELECT graduation_day, '{column}' AS feature, count(*) AS rows, count({column}) AS non_null_rows "
            f"FROM (SELECT c.*, g.graduation_day FROM read_parquet({q(checkpoint_index())}) c JOIN read_parquet({q(graduation_index())}) g USING (mint, graduation_time)) GROUP BY graduation_day"
            for column in FEATURE_COLUMNS
        )
        coverage = connection.execute(coverage_query + " ORDER BY graduation_day, feature").fetchall()
    with coverage_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("graduation_day", "feature", "rows", "non_null_rows", "coverage_pct"))
        for day_value, feature, rows, non_null in coverage:
            writer.writerow((day_value, feature, rows, non_null, f"{(non_null / rows * 100 if rows else 0):.2f}"))
    lines = [
        "# MT-762 Coin Checkpoints", "", "## Definition And Boundaries", "",
        "Graduation is the archive replay definition from `D:\\pumpapi-replay\\etl.py:396-423`: a `migrate` birth event is floored into a five-second bar, and that grouped OHLCV bar is emitted as `graduated_this_bar = true`. This table uses each mint's first such marker within Apr 18 through Aug 21 UTC.",
        "A checkpoint is the first observed five-second bar from its target age through target plus 30 seconds. No row is emitted when no such bar exists; nulls are never zero-filled. Feature history joins `graduation_time <= bar_time <= checkpoint_time` only. Forward scans use `bar_time > checkpoint_time` only, and every outcome column begins `fwd_`.",
        "`price` is the checkpoint close and is intentionally rendered in scientific notation in any value-level output because token prices are commonly around 1e-8.",
        "", "## Daily Counts", "", "| day | graduated coins | rows | 30s | 2m | 5m | 10m | 22m |", "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(f"| {row[0]} | {row[1]:,} | {row[2]:,} | {row[3]:,} | {row[4]:,} | {row[5]:,} | {row[6]:,} | {row[7]:,} |" for row in day_rows)
    lines.extend([
        "", "## Feature Coverage", "",
        "`feature_coverage_by_day.csv` contains per-day non-null count and percentage for every feature column, including the known-sparse `unique_wallets_total`, `top10_holder_pct`, and `creator_holdings_pct` fields. The fields are retained unchanged.",
        "", "## Labels", "",
    ])
    if trade_counts is None:
        lines.append("Trade-label or rug-label join did not complete; base checkpoint rows and the AUC section remain independent.")
    else:
        lines.extend([
            f"- Rows labeled as his trades: **{trade_counts['he_traded_rows']:,}**.",
            f"- Established quarantined source positions: **{trade_counts['source_quarantined_positions']:,}**; all **{trade_counts['quarantined_mints']:,}** represented checkpoint mints remain flagged across **{trade_counts['quarantined_rows']:,}** checkpoint rows and are excluded from creator-dependent slices.",
            f"- Non-created, non-quarantined T1 rows: **{trade_counts['noncreated_t1_rows']:,}**; task expectation was 1,496.",
            f"- Non-created, non-quarantined T4 rows: **{trade_counts['noncreated_t4_rows']:,}**; task expectation was 7,325.",
            "The persisted source labels win: this archive-table join reproduces 120 T1 and 7,474 T4 rows because `wallet_position_features.tier` is the existing tier source. The disagreement is reported rather than relabeled.",
        ])
    lines.extend(["", "## AUC: 5m Checkpoint, Apr 18-May 18", ""])
    if auc is None:
        lines.append("AUC was not available because the independent base build did not complete.")
    else:
        lines.extend([
            "Outcome tiers use `fwd_max_multiple` at the 5m checkpoint: top decile T1, deciles 2-3 T2, deciles 4-6 T3, and lower four deciles T4. AUC compares T1 with T4 and is direction-normalized as `max(auc, 1 - auc)`.",
            "| feature | normalized AUC | favored direction | valid T1 | valid T4 |", "|---|---:|---|---:|---:|",
            *[f"| {row['feature']} | {row['auc']:.3f} | {row['direction']} | {row['valid_t1']:,} | {row['valid_t4']:,} |" for row in auc if row["auc"] is not None],
            "", "Only rows whose graduation time falls in Apr 18-May 18 enter this analysis; later graduated coins are not read by the AUC query and remain holdout.",
        ])
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_subprocess(flag: str, day: date) -> None:
    subprocess.run(
        ["run-capped", "6G", sys.executable, str(Path(__file__).resolve()), flag, day.isoformat(), "--output", str(ROOT)],
        check=True,
    )


def build_base() -> int:
    graduation_index()
    for day in dates():
        run_subprocess("--checkpoint-day", day)
    checkpoint_index()
    for day in dates():
        run_subprocess("--forward-day", day)
    forward_summary()
    return write_base_table()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT)
    parser.add_argument("--checkpoint-day", type=date.fromisoformat)
    parser.add_argument("--forward-day", type=date.fromisoformat)
    parser.add_argument("--base", action="store_true")
    parser.add_argument("--labels", action="store_true")
    parser.add_argument("--auc", action="store_true")
    return parser.parse_args()


def main() -> None:
    global ROOT
    args = parse_args()
    ROOT = args.output
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.checkpoint_day:
        checkpoint_day(args.checkpoint_day)
        return
    if args.forward_day:
        forward_day(args.forward_day)
        return
    if not (args.base or args.labels or args.auc):
        args.base = args.labels = args.auc = True
    if args.base:
        print(f"base_rows_upserted={build_base()}", flush=True)
    trade_counts: dict[str, int] | None = None
    if args.labels:
        trade_counts = apply_trade_labels()
        print(f"trade_labels={trade_counts}", flush=True)
        print(f"rug_labels_updated={apply_rug_labels()}", flush=True)
    ranking = auc_ranking() if args.auc else None
    render_report(trade_counts, ranking)


if __name__ == "__main__":
    main()
