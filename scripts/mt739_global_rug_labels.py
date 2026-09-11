#!/usr/bin/env python3
"""Label first rug events across the full PumpApi archive with carried mint state."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb


ARCHIVE_ROOT = Path("/mnt/d/pumpapi-replay")
RESULTS_DIR = Path("/home/dev/workspace/data/results/mt739")
LEGACY_LABELS = Path("/home/dev/workspace/data/results/mt738/reimplemented_rug_labels.csv")
START = "2026-04-18"
END = "2026-08-21"
WINDOW_START = "2026-07-22"
WINDOW_END = "2026-08-21"
MIN_AVAILABLE_RAM_BYTES = 8_000_000_000


def date_range(start: str, end: str) -> list[str]:
    current = datetime.fromisoformat(start).replace(tzinfo=UTC)
    last = datetime.fromisoformat(end).replace(tzinfo=UTC)
    values = []
    while current <= last:
        values.append(current.date().isoformat())
        current += timedelta(days=1)
    return values


def sql_string(value: str | Path) -> str:
    return json.dumps(str(value))


def mem_available_bytes() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return 0


def open_database(path: Path, temp_dir: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(path))
    connection.execute("SET memory_limit = '2GB'")
    connection.execute("SET threads = 2")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(f"SET temp_directory = {sql_string(temp_dir)}")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS mint_state (
            mint VARCHAR PRIMARY KEY,
            all_time_peak_pool_sol DOUBLE NOT NULL,
            already_labeled BOOLEAN NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_days (
            processed_date DATE PRIMARY KEY,
            ticks_scanned BIGINT NOT NULL,
            labels_emitted BIGINT NOT NULL,
            cumulative_labels BIGINT NOT NULL,
            completed_at TIMESTAMP NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS legacy_tick_diagnostics (
            mint VARCHAR,
            rug_timestamp BIGINT,
            action VARCHAR,
            pool_sol_after DOUBLE,
            daily_prior_peak_sol DOUBLE,
            all_time_prior_peak_sol DOUBLE
        )
        """
    )
    return connection


def initialize_legacy_labels(connection: duckdb.DuckDBPyConnection, path: Path) -> None:
    exists = connection.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'legacy_labels'"
    ).fetchone()[0]
    if exists:
        return
    if not path.is_file():
        raise FileNotFoundError(f"MT-738 labels not found: {path}")
    connection.execute(
        f"""
        CREATE TABLE legacy_labels AS
        SELECT mint, rug_timestamp::BIGINT AS rug_timestamp,
               pool_sol_before::DOUBLE AS daily_prior_peak_sol,
               pool_sol_after::DOUBLE AS pool_sol_after
        FROM read_csv_auto({sql_string(path)}, header=true)
        """
    )
    connection.execute("CREATE INDEX legacy_labels_mint_timestamp ON legacy_labels(mint, rug_timestamp)")


def current_cumulative_labels(connection: duckdb.DuckDBPyConnection) -> int:
    return int(connection.execute("SELECT count(*) FROM mint_state WHERE already_labeled").fetchone()[0])


def process_day(
    connection: duckdb.DuckDBPyConnection,
    archive_root: Path,
    output_dir: Path,
    date: str,
) -> tuple[int, int, int]:
    tick_path = archive_root / "derived" / "ticks" / f"{date}.parquet"
    if not tick_path.is_file():
        raise FileNotFoundError(f"Missing archive day: {tick_path}")

    chunk_path = output_dir / "label_chunks" / f"{date}.parquet"
    # A failed pre-commit COPY can leave a partial chunk behind; the checkpoint
    # row is only written after a successful transaction, so it is safe to replace.
    chunk_path.unlink(missing_ok=True)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE day_ticks AS
            SELECT mint, timestamp::BIGINT AS timestamp, signature, lower(action) AS action,
                   sol_in_pool::DOUBLE AS sol_in_pool
            FROM read_parquet({sql_string(tick_path)})
            WHERE mint IS NOT NULL AND timestamp IS NOT NULL
            """
        )
        ticks_scanned = int(connection.execute("SELECT count(*) FROM day_ticks").fetchone()[0])
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE day_scan AS
            WITH prior_day AS (
                SELECT ticks.*, COALESCE(state.all_time_peak_pool_sol, 0.0) AS starting_peak_sol,
                       COALESCE(state.already_labeled, false) AS already_labeled
                FROM day_ticks AS ticks
                LEFT JOIN mint_state AS state USING (mint)
            ), scanned AS (
                SELECT *,
                       greatest(
                           starting_peak_sol,
                           COALESCE(max(sol_in_pool) FILTER (WHERE sol_in_pool > 0) OVER (
                               PARTITION BY mint ORDER BY timestamp, signature
                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                           ), 0.0)
                       ) AS all_time_prior_peak_sol
                FROM prior_day
            )
            SELECT * FROM scanned
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE day_labels AS
            WITH candidates AS (
                SELECT *, row_number() OVER (
                    PARTITION BY mint ORDER BY timestamp, signature
                ) AS label_rank
                FROM day_scan
                WHERE NOT already_labeled
                  AND (action = 'remove' OR sol_in_pool < all_time_prior_peak_sol * 0.01)
            )
            SELECT mint, timestamp AS rug_timestamp,
                   all_time_prior_peak_sol AS pool_sol_before,
                   sol_in_pool AS pool_sol_after,
                   CASE WHEN action = 'remove' THEN 'remove' ELSE 'pool_below_1pct_peak' END AS label_reason
            FROM candidates
            WHERE label_rank = 1
            """
        )
        labels_emitted = int(connection.execute("SELECT count(*) FROM day_labels").fetchone()[0])
        # These are the MT-738 label ticks only. Capturing their carried peak here
        # makes the later daily-peak versus all-time-peak decomposition auditable.
        connection.execute(
            """
            INSERT INTO legacy_tick_diagnostics
            SELECT scan.mint, scan.timestamp, scan.action, scan.sol_in_pool,
                   legacy.daily_prior_peak_sol, scan.all_time_prior_peak_sol
            FROM day_scan AS scan
            JOIN legacy_labels AS legacy
              ON legacy.mint = scan.mint AND legacy.rug_timestamp = scan.timestamp
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE day_state_update AS
            SELECT ticks.mint,
                   COALESCE(max(ticks.sol_in_pool) FILTER (WHERE ticks.sol_in_pool > 0), 0.0) AS day_peak_sol,
                   COALESCE(labels.mint IS NOT NULL, false) AS labeled_today
            FROM day_ticks AS ticks
            LEFT JOIN day_labels AS labels USING (mint)
            GROUP BY ticks.mint, labeled_today
            """
        )
        connection.execute(
            """
            MERGE INTO mint_state AS state
            USING day_state_update AS update_row
            ON state.mint = update_row.mint
            WHEN MATCHED THEN UPDATE SET
                all_time_peak_pool_sol = greatest(state.all_time_peak_pool_sol, update_row.day_peak_sol),
                already_labeled = state.already_labeled OR update_row.labeled_today
            WHEN NOT MATCHED THEN INSERT (mint, all_time_peak_pool_sol, already_labeled)
                VALUES (update_row.mint, update_row.day_peak_sol, update_row.labeled_today)
            """
        )
        connection.execute(
            f"COPY day_labels TO {sql_string(chunk_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        cumulative = current_cumulative_labels(connection)
        connection.execute(
            """
            INSERT INTO processed_days VALUES (?, ?, ?, ?, current_timestamp)
            """,
            [date, ticks_scanned, labels_emitted, cumulative],
        )
        connection.execute("COMMIT")
        return ticks_scanned, labels_emitted, cumulative
    except Exception:
        connection.execute("ROLLBACK")
        raise


def write_summary(connection: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    chunks = sorted((output_dir / "label_chunks").glob("*.parquet"))
    if not chunks:
        raise RuntimeError("No label chunks were written")
    source = ", ".join(sql_string(path) for path in chunks)
    connection.execute(
        f"""
        COPY (
            SELECT mint, rug_timestamp, pool_sol_before, pool_sol_after, label_reason
            FROM read_parquet([{source}])
            ORDER BY rug_timestamp, mint
        ) TO {sql_string(output_dir / 'global_rug_labels.csv')} (HEADER, DELIMITER ',')
        """
    )
    window_start_ms = int(datetime(2026, 7, 22, tzinfo=UTC).timestamp() * 1000)
    window_end_ms = int(datetime(2026, 8, 22, tzinfo=UTC).timestamp() * 1000)
    summary = connection.execute(
        f"""
        WITH global_labels AS (
            SELECT * FROM read_parquet([{source}])
        ), local_labels AS (
            SELECT * FROM legacy_labels
        ), window_global AS (
            SELECT * FROM global_labels
            WHERE rug_timestamp >= {window_start_ms} AND rug_timestamp < {window_end_ms}
        ), local_missing_global AS (
            SELECT local_labels.*
            FROM local_labels
            LEFT JOIN window_global USING (mint)
            WHERE window_global.mint IS NULL
        ), mechanism_a AS (
            SELECT missing.mint
            FROM local_missing_global AS missing
            JOIN global_labels USING (mint)
            WHERE global_labels.rug_timestamp < {window_start_ms}
        ), mechanism_b AS (
            SELECT missing.mint
            FROM local_missing_global AS missing
            JOIN legacy_tick_diagnostics AS diagnostic
              ON diagnostic.mint = missing.mint
             AND diagnostic.rug_timestamp = missing.rug_timestamp
            LEFT JOIN mechanism_a USING (mint)
            WHERE mechanism_a.mint IS NULL
              AND diagnostic.action <> 'remove'
              AND diagnostic.pool_sol_after < diagnostic.daily_prior_peak_sol * 0.01
              AND NOT diagnostic.pool_sol_after < diagnostic.all_time_prior_peak_sol * 0.01
        )
        SELECT
            (SELECT count(*) FROM global_labels) AS full_archive_labels,
            (SELECT count(*) FROM window_global) AS window_labels,
            (SELECT count(*) FROM local_labels) AS mt738_window_labels,
            (SELECT count(*) FROM mechanism_a) AS mechanism_a,
            (SELECT count(*) FROM mechanism_b) AS mechanism_b,
            (SELECT count(*) FROM local_missing_global) AS local_missing_global,
            (SELECT count(*) FROM window_global
             LEFT JOIN local_labels USING (mint)
             WHERE local_labels.mint IS NULL) AS extra_global_window_mints,
            (SELECT count(*) FROM legacy_tick_diagnostics) AS legacy_ticks_observed
        """
    ).fetchone()
    keys = [item[0] for item in connection.description]
    payload = dict(zip(keys, summary, strict=True))
    payload["difference_from_mt738"] = payload["mt738_window_labels"] - payload["window_labels"]
    payload["residual"] = (
        payload["difference_from_mt738"]
        - payload["mechanism_a"]
        - payload["mechanism_b"]
    )
    payload["window_start"] = WINDOW_START
    payload["window_end_exclusive"] = "2026-08-22"
    (output_dir / "label_summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--legacy-labels", type=Path, default=LEGACY_LABELS)
    parser.add_argument("--start", default=START)
    parser.add_argument("--end", default=END)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "label_chunks").mkdir(exist_ok=True)
    temp_dir = output_dir / "duckdb_tmp"
    temp_dir.mkdir(exist_ok=True)
    lock_path = output_dir / ".lock"
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise SystemExit(f"Refusing to start: lock held at {lock_path}") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as lock:
        lock.write(f"pid={os.getpid()} started_at={datetime.now(UTC).isoformat()}\n")
    started = time.monotonic()
    try:
        with open_database(output_dir / "state.duckdb", temp_dir) as connection:
            initialize_legacy_labels(connection, args.legacy_labels)
            completed = {
                str(row[0])
                for row in connection.execute("SELECT processed_date FROM processed_days").fetchall()
            }
            for date in date_range(args.start, args.end):
                if date in completed:
                    continue
                available = mem_available_bytes()
                if available and available < MIN_AVAILABLE_RAM_BYTES:
                    with (output_dir / "progress.log").open("a", encoding="utf-8", buffering=1) as log:
                        log.write(
                            f"{datetime.now(UTC).isoformat()} backoff available_ram_bytes={available} "
                            f"before_date={date} elapsed_s={time.monotonic() - started:.1f}\n"
                        )
                    raise SystemExit("Checkpoint complete; available RAM fell below 8GB, retry later.")
                ticks, labels, cumulative = process_day(connection, args.root, output_dir, date)
                with (output_dir / "progress.log").open("a", encoding="utf-8", buffering=1) as log:
                    log.write(
                        f"{datetime.now(UTC).isoformat()} date={date} ticks_scanned={ticks} "
                        f"labels_emitted={labels} cumulative_labels={cumulative} "
                        f"elapsed_s={time.monotonic() - started:.1f}\n"
                    )
            if len(completed) + sum(date not in completed for date in date_range(args.start, args.end)) == len(date_range(args.start, args.end)):
                write_summary(connection, output_dir)
    finally:
        lock_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
