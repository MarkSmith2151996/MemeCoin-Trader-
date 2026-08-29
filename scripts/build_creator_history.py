#!/usr/bin/env python3
"""Build the daily, prior-only creator history snapshot in Hive.

The archive's ``reached_2x`` outcome is the only available historical success label.
``prior_rug_rate`` therefore means the share of outcome-known prior launches that did
not reach 2x. It is data-only metadata, not a blockchain-proven rug classification.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import asyncpg
import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.store import database_dsn  # noqa: E402

DEFAULT_REPLAY_ROOT = Path(os.getenv("MEMECOIN_REPLAY_ROOT", "/mnt/d/pumpapi-replay"))
DEFAULT_OUTCOME_PATHS = (
    Path("results/token_outcomes.csv"),
    Path("results/extended_holdout_outcomes.csv"),
)
COPY_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class CreatorHistoryRow:
    creator_wallet: str
    prior_deploy_count: int
    prior_rug_observation_count: int
    prior_rug_count: int
    prior_rug_rate: float | None


def sql_path(path: Path) -> str:
    """Return a DuckDB-safe path literal body."""

    return str(path).replace("'", "''")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument(
        "--outcomes",
        type=Path,
        action="append",
        help="Outcome CSV path; repeat to override the two default archive files.",
    )
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
        help="UTC snapshot date (YYYY-MM-DD); same-day births are excluded.",
    )
    return parser.parse_args()


def resolve_outcome_paths(replay_root: Path, supplied: Sequence[Path] | None) -> list[Path]:
    paths = list(supplied) if supplied else [replay_root / path for path in DEFAULT_OUTCOME_PATHS]
    missing = [path for path in paths if not path.is_file()]
    if missing:
        rendered = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing creator-history outcome file(s): {rendered}")
    return paths


def _snapshot_query(
    replay_root: Path,
    outcome_paths: Sequence[Path],
    as_of_date: date,
) -> tuple[Any, date | None]:
    births_glob = replay_root / "derived" / "births" / "*.parquet"
    if not list(births_glob.parent.glob(births_glob.name)):
        raise FileNotFoundError(f"No birth parquet files found at {births_glob}")
    as_of_ms = int(datetime.combine(as_of_date, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)
    outcomes_sql = ", ".join(f"'{sql_path(path)}'" for path in outcome_paths)
    temporary_dir = replay_root / "derived" / ".creator-history-duckdb-tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET memory_limit = '2GB'")
    connection.execute(f"SET temp_directory = '{sql_path(temporary_dir)}'")
    connection.execute("SET threads = 2")
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(
        f"""
        CREATE TEMP TABLE births AS
        WITH launch_rows AS (
            SELECT mint, creator_wallet, timestamp, signature,
                   row_number() OVER (
                       PARTITION BY mint ORDER BY timestamp, signature
                   ) AS launch_rank
            FROM read_parquet('{sql_path(births_glob)}')
            WHERE mint IS NOT NULL
              AND creator_wallet IS NOT NULL
              AND timestamp < {as_of_ms}
              AND lower(action) IN ('create', 'createpool')
        )
        SELECT mint, creator_wallet, timestamp
        FROM launch_rows
        WHERE launch_rank = 1
        """
    )
    source_through = connection.execute(
        "SELECT max(CAST(to_timestamp(timestamp / 1000) AS DATE)) FROM births"
    ).fetchone()[0]
    connection.execute(
        f"""
        CREATE TEMP TABLE outcomes AS
        WITH source_rows AS (
            SELECT mint, birth_timestamp, try_cast(reached_2x AS BOOLEAN) AS reached_2x,
                   filename,
                   row_number() OVER (
                       PARTITION BY mint ORDER BY filename
                   ) AS outcome_rank
            FROM read_csv_auto(
                [{outcomes_sql}], HEADER = true, union_by_name = true, filename = true
            )
            WHERE mint IS NOT NULL
              AND birth_timestamp < {as_of_ms}
        )
        SELECT mint, reached_2x
        FROM source_rows
        WHERE outcome_rank = 1
        """
    )
    return (
        connection.execute(
            """
            SELECT
                b.creator_wallet,
                count(*)::INTEGER AS prior_deploy_count,
                count(o.reached_2x)::INTEGER AS prior_rug_observation_count,
                count(*) FILTER (WHERE o.reached_2x IS FALSE)::INTEGER AS prior_rug_count,
                count(*) FILTER (WHERE o.reached_2x IS FALSE)::DOUBLE PRECISION
                    / nullif(count(o.reached_2x), 0) AS prior_rug_rate
            FROM births AS b
            LEFT JOIN outcomes AS o USING (mint)
            GROUP BY b.creator_wallet
            ORDER BY b.creator_wallet
            """
        ),
        source_through,
    )


def rows_from_cursor(cursor: Any) -> Iterator[CreatorHistoryRow]:
    """Yield copy-ready rows without materializing the full creator population."""

    while rows := cursor.fetchmany(COPY_BATCH_SIZE):
        for creator, deployments, observations, rugs, rug_rate in rows:
            yield CreatorHistoryRow(
                creator_wallet=str(creator),
                prior_deploy_count=int(deployments),
                prior_rug_observation_count=int(observations),
                prior_rug_count=int(rugs),
                prior_rug_rate=float(rug_rate) if rug_rate is not None else None,
            )


async def replace_creator_history(
    dsn: str,
    rows: Iterator[CreatorHistoryRow],
    *,
    as_of_date: date,
    source_through_date: date | None,
) -> int:
    """Atomically replace the current snapshot using data-role DML only."""

    written = 0

    def records() -> Iterator[tuple[object, ...]]:
        nonlocal written
        for row in rows:
            written += 1
            yield (
                row.creator_wallet,
                as_of_date,
                source_through_date,
                row.prior_deploy_count,
                row.prior_rug_observation_count,
                row.prior_rug_count,
                row.prior_rug_rate,
            )

    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction():
            await connection.execute("DELETE FROM memecoin.creator_history")
            await connection.copy_records_to_table(
                "creator_history",
                schema_name="memecoin",
                columns=(
                    "creator_wallet",
                    "as_of_date",
                    "source_through_date",
                    "prior_deploy_count",
                    "prior_rug_observation_count",
                    "prior_rug_count",
                    "prior_rug_rate",
                ),
                records=records(),
            )
    finally:
        await connection.close()
    return written


async def run(args: argparse.Namespace) -> int:
    replay_root = args.replay_root.resolve()
    outcome_paths = resolve_outcome_paths(replay_root, args.outcomes)
    cursor, source_through = _snapshot_query(replay_root, outcome_paths, args.as_of_date)
    try:
        rows = rows_from_cursor(cursor)
        written = await replace_creator_history(
            database_dsn(),
            rows,
            as_of_date=args.as_of_date,
            source_through_date=source_through,
        )
    finally:
        cursor.close()
    print(
        "creator history refreshed "
        f"as_of={args.as_of_date.isoformat()} source_through={source_through} creators={written}"
    )
    return written


def main() -> None:
    load_dotenv(ROOT / ".env")
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
