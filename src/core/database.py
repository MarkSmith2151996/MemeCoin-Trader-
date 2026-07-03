"""Async SQLite persistence helpers for trades and positions."""

from __future__ import annotations

from pathlib import Path

import aiosqlite

from src.core.models import Position, Trade


SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS trades (
      id TEXT PRIMARY KEY,
      mint_address TEXT NOT NULL,
      side TEXT NOT NULL,
      amount_sol REAL NOT NULL,
      token_amount REAL,
      price_sol REAL,
      slippage_bps INTEGER NOT NULL,
      tx_signature TEXT,
      mode TEXT NOT NULL,
      status TEXT NOT NULL,
      executed_at TEXT NOT NULL,
      metadata_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
      id TEXT PRIMARY KEY,
      mint_address TEXT NOT NULL,
      entry_trade_id TEXT NOT NULL,
      amount_sol REAL NOT NULL,
      token_amount REAL NOT NULL,
      entry_price_sol REAL NOT NULL,
      status TEXT NOT NULL,
      opened_at TEXT NOT NULL,
      closed_at TEXT,
      realized_pnl_sol REAL NOT NULL,
      partial_exits_json TEXT NOT NULL
    )
    """,
)


async def init_db(path: str | Path) -> None:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        for statement in SCHEMA:
            await db.execute(statement)
        await db.commit()


async def record_trade(path: str | Path, trade: Trade) -> None:
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.id,
                trade.mint_address,
                trade.side.value,
                trade.amount_sol,
                trade.token_amount,
                trade.price_sol,
                trade.slippage_bps,
                trade.tx_signature,
                trade.mode,
                trade.status,
                trade.executed_at.isoformat(),
                trade.model_dump_json(),
            ),
        )
        await db.commit()


async def record_position(path: str | Path, position: Position) -> None:
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position.id,
                position.mint_address,
                position.entry_trade_id,
                position.amount_sol,
                position.token_amount,
                position.entry_price_sol,
                position.status.value,
                position.opened_at.isoformat(),
                position.closed_at.isoformat() if position.closed_at else None,
                position.realized_pnl_sol,
                position.model_dump_json(),
            ),
        )
        await db.commit()
