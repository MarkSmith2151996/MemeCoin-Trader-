"""Async SQLite persistence helpers for trades and positions."""

from __future__ import annotations

from pathlib import Path

import aiosqlite

from src.core.models import PaperDecisionRecord, Position, SoakRunRecord, Trade


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
      partial_exits_json TEXT NOT NULL,
      close_price_sol REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_decisions (
      id TEXT PRIMARY KEY,
      recorded_at TEXT NOT NULL,
      cycle_id TEXT NOT NULL,
      execution_mode TEXT NOT NULL,
      risk_profile TEXT NOT NULL,
      mint_address TEXT NOT NULL,
      symbol TEXT,
      name TEXT,
      source TEXT NOT NULL,
      source_count INTEGER NOT NULL,
      candidate_mode TEXT NOT NULL,
      decision TEXT NOT NULL,
      action_outcome TEXT NOT NULL,
      primary_reason TEXT NOT NULL,
      attention_score INTEGER NOT NULL,
      risk_score REAL,
      diagnostics_json TEXT NOT NULL,
      record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_soak_runs (
      id TEXT PRIMARY KEY,
      started_at TEXT NOT NULL,
      completed_at TEXT,
      max_signals INTEGER NOT NULL,
      timeout_seconds REAL NOT NULL,
      execution_mode TEXT NOT NULL,
      risk_profile TEXT NOT NULL,
      signals_collected INTEGER NOT NULL,
      signals_accepted INTEGER NOT NULL,
      signals_rejected INTEGER NOT NULL,
      trades_persisted INTEGER NOT NULL,
      open_positions INTEGER NOT NULL,
      source_failures_json TEXT NOT NULL,
      rejection_reasons_json TEXT NOT NULL,
      capacity_blocked INTEGER NOT NULL,
      unknown_data_blocks INTEGER NOT NULL,
      unexpected_failures INTEGER NOT NULL,
      termination_reason TEXT NOT NULL,
      elapsed_seconds REAL NOT NULL,
      health_ok INTEGER NOT NULL,
      health_message TEXT NOT NULL,
      guardrail_diagnostics_json TEXT NOT NULL,
      circuit_breaker_diagnostics_json TEXT NOT NULL,
      readiness_json TEXT NOT NULL
    )
    """,
)


async def init_db(path: str | Path) -> None:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        for statement in SCHEMA:
            await db.execute(statement)
        try:
            await db.execute("ALTER TABLE positions ADD COLUMN close_price_sol REAL")
        except aiosqlite.OperationalError:
            pass
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
            INSERT OR REPLACE INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                position.close_price_sol,
            ),
        )
        await db.commit()


async def record_paper_decision(path: str | Path, record: PaperDecisionRecord) -> None:
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO paper_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.recorded_at,
                record.cycle_id,
                record.execution_mode,
                record.risk_profile,
                record.mint_address,
                record.symbol,
                record.name,
                record.source,
                record.source_count,
                record.candidate_mode,
                record.decision,
                record.action_outcome,
                record.primary_reason,
                record.attention_score,
                record.risk_score,
                record.diagnostics_json,
                record.model_dump_json(),
            ),
        )
        await db.commit()


async def get_recent_paper_decisions(path: str | Path, limit: int = 50) -> list[PaperDecisionRecord]:
    async with aiosqlite.connect(path) as db:
        cursor = await db.execute(
            "SELECT record_json FROM paper_decisions ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
    return [PaperDecisionRecord.model_validate_json(row[0]) for row in rows]


async def record_soak_run(path: str | Path, record: SoakRunRecord) -> None:
    async with aiosqlite.connect(path) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO paper_soak_runs VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                record.id,
                record.started_at,
                record.completed_at,
                record.max_signals,
                record.timeout_seconds,
                record.execution_mode,
                record.risk_profile,
                record.signals_collected,
                record.signals_accepted,
                record.signals_rejected,
                record.trades_persisted,
                record.open_positions,
                record.source_failures_json,
                record.rejection_reasons_json,
                record.capacity_blocked,
                record.unknown_data_blocks,
                record.unexpected_failures,
                record.termination_reason,
                record.elapsed_seconds,
                1 if record.health_ok else 0,
                record.health_message,
                record.guardrail_diagnostics_json,
                record.circuit_breaker_diagnostics_json,
                record.readiness_json,
            ),
        )
        await db.commit()


async def get_recent_soak_runs(path: str | Path, limit: int = 5) -> list[SoakRunRecord]:
    async with aiosqlite.connect(path) as db:
        cursor = await db.execute(
            "SELECT * FROM paper_soak_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
    columns = [
        "id", "started_at", "completed_at", "max_signals", "timeout_seconds",
        "execution_mode", "risk_profile", "signals_collected", "signals_accepted",
        "signals_rejected", "trades_persisted", "open_positions",
        "source_failures_json", "rejection_reasons_json", "capacity_blocked",
        "unknown_data_blocks", "unexpected_failures", "termination_reason",
        "elapsed_seconds", "health_ok", "health_message",
        "guardrail_diagnostics_json", "circuit_breaker_diagnostics_json",
        "readiness_json",
    ]
    results: list[SoakRunRecord] = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        row_dict["health_ok"] = bool(row_dict["health_ok"])
        results.append(SoakRunRecord(**row_dict))
    return results
