"""Build resumable Parquet tables from PumpApi replay JSONL archives on Windows."""

from __future__ import annotations

import argparse
import io
import json
import logging
import math
import os
import sqlite3
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import zstandard

ARCHIVE_ROOT = Path(r"D:\pumpapi-replay")
WORKERS = 4
ROW_BATCH_SIZE = 100_000
TRADE_ACTIONS = {"buy", "sell", "add", "remove"}
BIRTH_ACTIONS = {"create", "createpool", "migrate"}

TRADE_SCHEMA = pa.schema(
    [
        ("mint", pa.string()),
        ("timestamp", pa.int64()),
        ("action", pa.string()),
        ("price", pa.float64()),
        ("mcap", pa.float64()),
        ("sol_amount", pa.float64()),
        ("pool", pa.string()),
    ]
)
BIRTH_SCHEMA = pa.schema(
    [
        ("mint", pa.string()),
        ("created_at", pa.int64()),
        ("action", pa.string()),
        ("pool", pa.string()),
        ("name", pa.string()),
        ("creator", pa.string()),
    ]
)


@dataclass(frozen=True)
class ArchiveFile:
    path: Path
    key: str
    date: str


@dataclass(frozen=True)
class ProcessResult:
    archive: ArchiveFile
    total_events: int
    kept_events: int
    birth_events: int
    invalid_events: int
    pool_events: Counter[str]
    pool_trades: Counter[str]
    pool_births: Counter[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--derived-dir", type=Path)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument(
        "--limit-hours",
        type=int,
        help="Process at most this many incomplete hourly archives; intended for a smoke test.",
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--candidate-mint", help="Optional mint whose derived trades must exist.")
    return parser.parse_args()


def configure_logging(derived_dir: Path) -> logging.Logger:
    log_dir = derived_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pumpapi_replay_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler = RotatingFileHandler(
        log_dir / "etl.log", maxBytes=50_000_000, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger


def atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def default_state() -> dict[str, Any]:
    return {
        "version": 1,
        "completed_hours": [],
        "last_completed_hour": None,
        "total_events": 0,
        "kept_events": 0,
        "birth_events": 0,
        "invalid_events": 0,
        "pool_event_counts": {},
        "pool_trade_counts": {},
        "pool_birth_counts": {},
        "birth_days_ingested": [],
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot safely load ETL state {path}: {exc}") from exc
    if not isinstance(state, dict) or state.get("version") != 1:
        raise RuntimeError(f"Unsupported ETL state format in {path}")
    defaults = default_state()
    for key, value in defaults.items():
        state.setdefault(key, value)
    if not isinstance(state["completed_hours"], list) or not all(
        isinstance(value, str) for value in state["completed_hours"]
    ):
        raise RuntimeError(f"Invalid completed_hours in {path}")
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat()
    atomic_json_write(path, state)


def archive_files(raw_dir: Path) -> list[ArchiveFile]:
    archives: list[ArchiveFile] = []
    for path in sorted(raw_dir.glob("*/*/*/*.jsonl.zst")):
        try:
            relative = path.relative_to(raw_dir)
            year, month, day, filename = relative.parts
            hour = filename.removesuffix(".jsonl.zst")
            int(year), int(month), int(day), int(hour)
        except (ValueError, AttributeError):
            continue
        archives.append(
            ArchiveFile(path=path, key=f"{year}/{month}/{day}/{hour}", date=f"{year}-{month}-{day}")
        )
    return archives


def as_text(value: Any) -> str | None:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result > 0 else None


def as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def event_pool(event: dict[str, Any]) -> str | None:
    return as_text(event.get("pool")) or as_text(event.get("poolType"))


def event_sol_amount(event: dict[str, Any]) -> float | None:
    for key in ("solAmount", "sol_amount", "sol", "amountSol", "quoteAmount"):
        result = as_float(event.get(key))
        if result is not None:
            return result
    breakdown = event.get("breakdown")
    if not isinstance(breakdown, list):
        return None
    for item in breakdown:
        if not isinstance(item, dict):
            continue
        symbol = as_text(item.get("symbol")) or as_text(item.get("mint"))
        sol_symbols = {"SOL", "WSOL", "SO11111111111111111111111111111111111111112"}
        if symbol and symbol.upper() in sol_symbols:
            for key in ("amount", "value", "uiAmount", "ui_amount"):
                result = as_float(item.get(key))
                if result is not None:
                    return abs(result)
    return None


def event_creator(event: dict[str, Any]) -> str | None:
    for key in (
        "creator",
        "creatorWallet",
        "creator_wallet",
        "creatorFeeAddress",
        "txSigner",
        "user",
        "owner",
        "wallet",
    ):
        value = as_text(event.get(key))
        if value:
            return value
    return None


def event_name(event: dict[str, Any]) -> str | None:
    for key in ("name", "tokenName", "token_name"):
        value = as_text(event.get(key))
        if value:
            return value
    return None


def table_from_rows(rows: dict[str, list[Any]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pydict(rows, schema=schema)


def append_row(rows: dict[str, list[Any]], values: dict[str, Any]) -> None:
    for key, value in values.items():
        rows[key].append(value)


def empty_rows(schema: pa.Schema) -> dict[str, list[Any]]:
    return {field.name: [] for field in schema}


def write_batch(writer: pq.ParquetWriter, rows: dict[str, list[Any]], schema: pa.Schema) -> None:
    if rows[next(iter(rows))]:
        writer.write_table(table_from_rows(rows, schema))
        for values in rows.values():
            values.clear()


def process_archive(archive: ArchiveFile, staging_dir: Path) -> ProcessResult:
    trade_path = staging_dir / "trades" / archive.date / f"{archive.key[-2:]}.parquet"
    birth_path = staging_dir / "births" / archive.date / f"{archive.key[-2:]}.parquet"
    trade_path.parent.mkdir(parents=True, exist_ok=True)
    birth_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_trade = trade_path.with_suffix(".parquet.tmp")
    temporary_birth = birth_path.with_suffix(".parquet.tmp")
    temporary_trade.unlink(missing_ok=True)
    temporary_birth.unlink(missing_ok=True)

    total_events = kept_events = birth_events = invalid_events = 0
    pool_events: Counter[str] = Counter()
    pool_trades: Counter[str] = Counter()
    pool_births: Counter[str] = Counter()
    trade_rows = empty_rows(TRADE_SCHEMA)
    birth_rows = empty_rows(BIRTH_SCHEMA)

    try:
        with (
            archive.path.open("rb") as compressed,
            zstandard.ZstdDecompressor().stream_reader(compressed) as decompressed,
            io.BufferedReader(decompressed) as reader,
            pq.ParquetWriter(temporary_trade, TRADE_SCHEMA, compression="snappy") as trade_writer,
            pq.ParquetWriter(temporary_birth, BIRTH_SCHEMA, compression="snappy") as birth_writer,
        ):
            for line in reader:
                total_events += 1
                try:
                    event = orjson.loads(line)
                except orjson.JSONDecodeError:
                    invalid_events += 1
                    continue
                if not isinstance(event, dict):
                    invalid_events += 1
                    continue
                # The replay uses txType; action is accepted for normalized exports.
                action = as_text(event.get("action")) or as_text(event.get("txType"))
                pool = event_pool(event)
                if pool:
                    pool_events[pool] += 1
                if not action:
                    continue
                normalized_action = action.lower()
                mint = as_text(event.get("mint"))
                timestamp = as_int(event.get("timestamp"))
                if normalized_action in TRADE_ACTIONS:
                    if not mint or timestamp is None:
                        invalid_events += 1
                        continue
                    append_row(
                        trade_rows,
                        {
                            "mint": mint,
                            "timestamp": timestamp,
                            "action": action,
                            "price": as_float(event.get("price")),
                            "mcap": as_float(event.get("marketCapQuote")),
                            "sol_amount": event_sol_amount(event),
                            "pool": pool,
                        },
                    )
                    kept_events += 1
                    pool_trades[pool or "unknown"] += 1
                    if len(trade_rows["mint"]) >= ROW_BATCH_SIZE:
                        write_batch(trade_writer, trade_rows, TRADE_SCHEMA)
                elif normalized_action in BIRTH_ACTIONS:
                    if not mint or timestamp is None:
                        invalid_events += 1
                        continue
                    append_row(
                        birth_rows,
                        {
                            "mint": mint,
                            "created_at": timestamp,
                            "action": action,
                            "pool": pool,
                            "name": event_name(event),
                            "creator": event_creator(event),
                        },
                    )
                    birth_events += 1
                    pool_births[pool or "unknown"] += 1
                    if len(birth_rows["mint"]) >= ROW_BATCH_SIZE:
                        write_batch(birth_writer, birth_rows, BIRTH_SCHEMA)
            write_batch(trade_writer, trade_rows, TRADE_SCHEMA)
            write_batch(birth_writer, birth_rows, BIRTH_SCHEMA)
        os.replace(temporary_trade, trade_path)
        os.replace(temporary_birth, birth_path)
    except Exception:
        temporary_trade.unlink(missing_ok=True)
        temporary_birth.unlink(missing_ok=True)
        raise
    return ProcessResult(
        archive=archive,
        total_events=total_events,
        kept_events=kept_events,
        birth_events=birth_events,
        invalid_events=invalid_events,
        pool_events=pool_events,
        pool_trades=pool_trades,
        pool_births=pool_births,
    )


def merge_daily_trades(staging_dir: Path, derived_dir: Path, date: str) -> int:
    fragments = sorted((staging_dir / "trades" / date).glob("*.parquet"))
    if not fragments:
        return 0
    destination = derived_dir / "trades" / f"{date}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".parquet.tmp")
    temporary.unlink(missing_ok=True)
    rows = 0
    try:
        with pq.ParquetWriter(temporary, TRADE_SCHEMA, compression="snappy") as writer:
            for fragment in fragments:
                parquet_file = pq.ParquetFile(fragment)
                for batch in parquet_file.iter_batches(batch_size=ROW_BATCH_SIZE):
                    rows += batch.num_rows
                    writer.write_batch(batch)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return rows


def open_birth_index(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS token_births (
            mint TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            action TEXT NOT NULL,
            pool TEXT,
            name TEXT,
            creator TEXT
        )
        """
    )
    return connection


def ingest_birth_day(connection: sqlite3.Connection, staging_dir: Path, date: str) -> int:
    fragments = sorted((staging_dir / "births" / date).glob("*.parquet"))
    inserted = 0
    for fragment in fragments:
        for batch in pq.ParquetFile(fragment).iter_batches(batch_size=ROW_BATCH_SIZE):
            rows = zip(
                batch.column(0).to_pylist(),
                batch.column(1).to_pylist(),
                batch.column(2).to_pylist(),
                batch.column(3).to_pylist(),
                batch.column(4).to_pylist(),
                batch.column(5).to_pylist(),
                strict=True,
            )
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO token_births (mint, created_at, action, pool, name, creator)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            inserted += connection.total_changes - before
    connection.commit()
    return inserted


def materialize_births(connection: sqlite3.Connection, destination: Path) -> int:
    temporary = destination.with_suffix(".parquet.tmp")
    temporary.unlink(missing_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    cursor = connection.execute(
        "SELECT mint, created_at, action, pool, name, creator "
        "FROM token_births ORDER BY created_at, mint"
    )
    try:
        with pq.ParquetWriter(temporary, BIRTH_SCHEMA, compression="snappy") as writer:
            while rows := cursor.fetchmany(ROW_BATCH_SIZE):
                count += len(rows)
                columns = list(zip(*rows, strict=True))
                writer.write_table(
                    pa.Table.from_arrays(
                        [
                            pa.array(column, type=field.type)
                            for column, field in zip(columns, BIRTH_SCHEMA, strict=True)
                        ],
                        schema=BIRTH_SCHEMA,
                    )
                )
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return count


def add_counts(target: dict[str, int], counts: Counter[str]) -> None:
    for key, value in counts.items():
        target[key] = int(target.get(key, 0)) + value


def completed_dates(archives: list[ArchiveFile], completed: set[str]) -> set[str]:
    by_date: dict[str, list[ArchiveFile]] = {}
    for archive in archives:
        by_date.setdefault(archive.date, []).append(archive)
    return {
        date
        for date, date_archives in by_date.items()
        if all(archive.key in completed for archive in date_archives)
    }


def validate(derived_dir: Path, candidate_mint: str | None, logger: logging.Logger) -> None:
    births_path = derived_dir / "token_births.parquet"
    trade_paths = sorted((derived_dir / "trades").glob("*.parquet"))
    if not births_path.exists() or not trade_paths:
        raise RuntimeError(
            "Derived output is incomplete: token_births.parquet or trade partitions are missing"
        )
    births = pq.ParquetFile(births_path)
    pools = (
        ds.dataset(births_path, format="parquet")
        .to_table(columns=["pool"])
        .group_by("pool")
        .aggregate([( "pool", "count")])
        .to_pylist()
    )
    trade_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in trade_paths)
    logger.info(
        "Validation: %d unique births, %d trade events, %d dates (%s to %s)",
        births.metadata.num_rows,
        trade_rows,
        len(trade_paths),
        trade_paths[0].stem,
        trade_paths[-1].stem,
    )
    logger.info(
        "Births by pool: %s",
        sorted(pools, key=lambda item: item["pool_count"], reverse=True),
    )
    if candidate_mint:
        matches = 0
        for path in trade_paths:
            table = pq.read_table(path, filters=[("mint", "=", candidate_mint)], columns=["mint"])
            matches += table.num_rows
        if not matches:
            raise RuntimeError(f"Candidate mint {candidate_mint} has no derived trade events")
        logger.info("Validation: candidate mint %s has %d trade events", candidate_mint, matches)


def run(args: argparse.Namespace) -> None:
    if args.workers < 1:
        raise ValueError("--workers must be at least one")
    if args.limit_hours is not None and args.limit_hours < 1:
        raise ValueError("--limit-hours must be at least one")
    root = args.root.resolve()
    raw_dir = (args.raw_dir or root / "raw").resolve()
    derived_dir = (args.derived_dir or root / "derived").resolve()
    logger = configure_logging(derived_dir)
    if args.validate_only:
        validate(derived_dir, args.candidate_mint, logger)
        return
    if not raw_dir.is_dir():
        raise RuntimeError(f"Raw archive directory is missing: {raw_dir}")
    archives = archive_files(raw_dir)
    if not archives:
        raise RuntimeError(f"No .jsonl.zst archives found under {raw_dir}")
    state_path = derived_dir / "etl_state.json"
    state = load_state(state_path)
    completed = set(state["completed_hours"])
    pending = [archive for archive in archives if archive.key not in completed]
    if args.limit_hours is not None:
        pending = pending[: args.limit_hours]
    logger.info(
        "Found %d raw hours; %d already completed; %d scheduled with %d workers",
        len(archives),
        len(completed),
        len(pending),
        args.workers,
    )
    staging_dir = derived_dir / ".staging"
    completed_count = len(completed)
    by_date: dict[str, list[ArchiveFile]] = {}
    scheduled_keys = {archive.key for archive in pending}
    for archive in archives:
        by_date.setdefault(archive.date, []).append(archive)
    birth_days_ingested = set(state["birth_days_ingested"])
    with open_birth_index(derived_dir / "token_births.sqlite") as birth_index:
        with ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="pumpapi-etl"
        ) as executor:
            for date, date_archives in sorted(by_date.items()):
                scheduled = [archive for archive in date_archives if archive.key in scheduled_keys]
                for result in executor.map(
                    lambda archive: process_archive(archive, staging_dir), scheduled
                ):
                    completed.add(result.archive.key)
                    state["completed_hours"] = sorted(completed)
                    state["last_completed_hour"] = result.archive.key
                    state["total_events"] += result.total_events
                    state["kept_events"] += result.kept_events
                    state["birth_events"] += result.birth_events
                    state["invalid_events"] += result.invalid_events
                    add_counts(state["pool_event_counts"], result.pool_events)
                    add_counts(state["pool_trade_counts"], result.pool_trades)
                    add_counts(state["pool_birth_counts"], result.pool_births)
                    save_state(state_path, state)
                    completed_count += 1
                    logger.info(
                        "[%d/%d] %s - %d events kept / %d total - %d births found",
                        completed_count,
                        len(archives),
                        result.archive.key,
                        result.kept_events,
                        result.total_events,
                        result.birth_events,
                    )
                if not all(archive.key in completed for archive in date_archives):
                    continue
                output_path = derived_dir / "trades" / f"{date}.parquet"
                if not scheduled and output_path.exists() and date in birth_days_ingested:
                    continue
                rows = merge_daily_trades(staging_dir, derived_dir, date)
                if date not in birth_days_ingested:
                    births = ingest_birth_day(birth_index, staging_dir, date)
                    birth_days_ingested.add(date)
                    state["birth_days_ingested"] = sorted(birth_days_ingested)
                    save_state(state_path, state)
                    logger.info("Finalized %s: %d trades, %d new unique births", date, rows, births)
                else:
                    logger.info("Finalized %s: %d trades", date, rows)
        if completed_dates(archives, completed):
            unique_births = materialize_births(birth_index, derived_dir / "token_births.parquet")
            logger.info("Materialized %d unique token births", unique_births)
    logger.info(
        "ETL pass complete: %d/%d raw hours complete, %d events kept, "
        "%d birth events, %d invalid events",
        len(completed),
        len(archives),
        state["kept_events"],
        state["birth_events"],
        state["invalid_events"],
    )
    logger.info("Event counts by pool: %s", dict(sorted(state["pool_event_counts"].items())))


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.exception("PumpApi ETL stopped: %s", exc)
        raise SystemExit(1) from exc
