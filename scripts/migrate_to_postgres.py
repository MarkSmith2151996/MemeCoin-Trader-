"""Explicit one-way import of historical SQLite trading records into Hive."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.store import database_dsn  # noqa: E402


def _uuid(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return str(uuid4())


def _datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def _float(value: object, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _strip_nuls(value: Any) -> Any:
    """Make nested JSON values valid for PostgreSQL JSONB."""

    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_nuls(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key).replace("\x00", ""): _strip_nuls(item)
            for key, item in value.items()
        }
    return value


def _json(value: object, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(value, dict):
        return _strip_nuls(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return default or {}
        return _strip_nuls(parsed) if isinstance(parsed, dict) else default or {}
    return default or {}


def _column(row: sqlite3.Row, name: str, default: object = None) -> object:
    return row[name] if name in row.keys() else default


async def _configure(connection: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await connection.set_type_codec(
            type_name,
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )


async def _apply_sql(connection: asyncpg.Connection, path: Path) -> None:
    await connection.execute(path.read_text())


async def migrate(sqlite_path: Path, dsn: str, *, apply_schema: bool) -> dict[str, int]:
    if not sqlite_path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")
    connection = await asyncpg.connect(dsn)
    await _configure(connection)
    try:
        if apply_schema:
            for name in (
                "create_memecoin_schema.sql",
                "seed_gate_config.sql",
                "seed_exit_config.sql",
            ):
                await _apply_sql(connection, PROJECT_ROOT / "sql" / name)

        sqlite = sqlite3.connect(sqlite_path)
        sqlite.row_factory = sqlite3.Row
        try:
            candidate_by_position, candidate_count = await _migrate_candidates(sqlite, connection)
            position_ids, position_count = await _migrate_positions(
                sqlite,
                connection,
                candidate_by_position,
            )
            trade_count = await _migrate_trades(sqlite, connection, position_ids)
        finally:
            sqlite.close()
        await connection.execute("SELECT memecoin.refresh_daily_stats($1)", "BT")
        return {"candidates": candidate_count, "positions": position_count, "trades": trade_count}
    finally:
        await connection.close()


async def _migrate_candidates(
    sqlite: sqlite3.Connection,
    connection: asyncpg.Connection,
) -> tuple[dict[str, int], int]:
    candidate_by_position: dict[str, int] = {}
    count = 0
    insert_query = """
        INSERT INTO memecoin.candidates (
            mint_address, observed_at, source, age_seconds, mcap_usd, volume_usd,
            txn_buys, txn_sells, buy_sell_ratio, liquidity_usd, fdv_usd, price_usd,
            creator_holdings_pct, price_change_5m, price_change_1h, raw_json
        ) VALUES (
            $1, $2, 'sqlite_candidate_log', $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15
        )
    """
    batch: list[tuple[object, ...]] = []

    async def flush() -> None:
        if batch:
            await connection.executemany(insert_query, batch)
            batch.clear()

    for row in sqlite.execute("SELECT * FROM candidate_log ORDER BY id"):
        raw = {
            "ticker": _column(row, "ticker"),
            "rugcheck_result": _column(row, "rugcheck_result"),
            "top10_holder_pct": _column(row, "top10_holder_pct"),
            "gates_passed": _column(row, "gates_passed"),
            "gates_failed": _column(row, "gates_failed"),
            "profile": _column(row, "profile"),
            "sqlite_candidate_id": _column(row, "id"),
        }
        values = (
            str(_column(row, "mint_address", "")),
            _datetime(_column(row, "scan_time")),
            (_float(_column(row, "age_minutes")) or 0) * 60,
            _float(_column(row, "mcap_usd")),
            _float(_column(row, "volume_usd")),
            _column(row, "txns_buys"),
            _column(row, "txns_sells"),
            _float(_column(row, "buy_sell_ratio")),
            _float(_column(row, "liquidity_usd")),
            _float(_column(row, "fdv")),
            _float(_column(row, "price_usd")),
            _float(_column(row, "dev_holdings_pct")),
            _float(_column(row, "price_change_5m")),
            _float(_column(row, "price_change_1h")),
            _strip_nuls(raw),
        )
        position_id = _column(row, "position_id")
        if position_id:
            await flush()
            candidate_id = await connection.fetchval(f"{insert_query} RETURNING id", *values)
            candidate_by_position[str(position_id)] = int(candidate_id)
        else:
            batch.append(values)
            if len(batch) >= 1000:
                await flush()
        count += 1
    await flush()
    return candidate_by_position, count


async def _migrate_positions(
    sqlite: sqlite3.Connection,
    connection: asyncpg.Connection,
    candidate_by_position: dict[str, int],
) -> tuple[dict[str, str], int]:
    position_ids: dict[str, str] = {}
    count = 0
    for row in sqlite.execute("SELECT * FROM positions ORDER BY opened_at"):
        legacy_id = str(_column(row, "id"))
        position_id = _uuid(legacy_id)
        position_ids[legacy_id] = position_id
        raw_status = str(_column(row, "status", "open")).lower()
        status = (
            "closed"
            if raw_status == "closed"
            else "abandoned"
            if raw_status == "abandoned"
            else "open"
        )
        entry_price = _float(_column(row, "entry_price_sol"), 0.0) or 0.0
        peak = _float(_column(row, "peak_price_sol"))
        await connection.execute(
            """
            INSERT INTO memecoin.positions (
                id, mint_address, entry_price_sol, amount_sol, token_amount, peak_price_sol,
                trailing_armed, status, mode, strategy, opened_at, closed_at,
                close_price_sol, realized_pnl_sol, adjusted_pnl_sol, candidate_id, fill_quality
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            ) ON CONFLICT DO NOTHING
            """,
            position_id,
            str(_column(row, "mint_address", "")),
            entry_price,
            _float(_column(row, "amount_sol"), 0.0) or 0.0,
            _float(_column(row, "token_amount")),
            peak,
            bool(peak and entry_price and peak >= entry_price * 1.02),
            status,
            str(_column(row, "mode", "paper") or "paper"),
            str(_column(row, "strategy", "BT") or "BT"),
            _datetime(_column(row, "opened_at")),
            _datetime(_column(row, "closed_at")) if _column(row, "closed_at") else None,
            _float(_column(row, "close_price_sol")),
            _float(_column(row, "realized_pnl_sol"), 0.0) or 0.0,
            _float(_column(row, "adjusted_pnl_sol")),
            candidate_by_position.get(legacy_id),
            "legacy_import",
        )
        count += 1
    return position_ids, count


async def _migrate_trades(
    sqlite: sqlite3.Connection,
    connection: asyncpg.Connection,
    position_ids: dict[str, str],
) -> int:
    positions_by_mint: dict[tuple[str, str], str] = {}
    for row in sqlite.execute("SELECT id, mint_address, mode FROM positions ORDER BY opened_at"):
        positions_by_mint[
            (str(row["mint_address"]), str(_column(row, "mode", "paper") or "paper"))
        ] = position_ids[str(row["id"])]
    entry_positions = {
        str(row["entry_trade_id"]): position_ids[str(row["id"])]
        for row in sqlite.execute("SELECT id, entry_trade_id FROM positions")
        if str(row["id"]) in position_ids
    }
    count = 0
    for row in sqlite.execute("SELECT * FROM trades ORDER BY executed_at"):
        mint = str(_column(row, "mint_address", ""))
        mode = str(_column(row, "mode", "paper") or "paper")
        legacy_trade_id = str(_column(row, "id"))
        position_id = entry_positions.get(legacy_trade_id) or positions_by_mint.get((mint, mode))
        await connection.execute(
            """
            INSERT INTO memecoin.trades (
                id, position_id, mint_address, side, amount_sol, token_amount, price_sol,
                slippage_bps, tx_signature, mode, executed_at, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
            """,
            _uuid(legacy_trade_id),
            position_id,
            mint,
            str(_column(row, "side", "unknown")).lower(),
            _float(_column(row, "amount_sol")),
            _float(_column(row, "token_amount")),
            _float(_column(row, "price_sol")),
            _column(row, "slippage_bps"),
            _column(row, "tx_signature"),
            mode,
            _datetime(_column(row, "executed_at")),
            _json(_column(row, "metadata_json")),
        )
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-path", type=Path, default=PROJECT_ROOT / "data" / "trades.db")
    parser.add_argument("--dsn", help="Hive PostgreSQL DSN; defaults to MEMECOIN_POSTGRES_DSN")
    parser.add_argument(
        "--apply", action="store_true", help="Perform the import; default is a safe preview"
    )
    parser.add_argument(
        "--apply-schema", action="store_true", help="Apply V2 schema and seeds before import"
    )
    args = parser.parse_args()
    load_dotenv()
    if not args.apply:
        connection = sqlite3.connect(args.sqlite_path)
        try:
            counts = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("candidate_log", "positions", "trades")
            }
        finally:
            connection.close()
        print(
            f"Preview only: {counts}. Re-run with --apply after configuring MEMECOIN_POSTGRES_DSN."
        )
        return
    if args.apply_schema and not args.apply:
        parser.error("--apply-schema requires --apply")
    result = asyncio.run(
        migrate(args.sqlite_path, args.dsn or database_dsn(), apply_schema=args.apply_schema)
    )
    print(f"Imported {result}")


if __name__ == "__main__":
    main()
