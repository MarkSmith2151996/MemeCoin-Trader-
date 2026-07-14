"""Async SQLite persistence helpers for trades and positions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiosqlite

from src.core.models import PaperDecisionRecord, Position, SoakRunRecord, Trade


@dataclass(frozen=True, slots=True)
class CandidateObservation:
    """Sanitized persisted identity and novelty state for one candidate mint."""

    id: str
    run_id: str
    observed_at: str
    mint_address: str
    first_seen_at: str
    seen_count_total: int
    seen_count_run: int
    is_new_mint: bool
    repeat_label: str


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
    """
    CREATE TABLE IF NOT EXISTS live_candidate_observations (
      id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      mint_address TEXT NOT NULL,
      source_names_json TEXT NOT NULL,
      source_event_types_json TEXT NOT NULL,
      candidate_mode TEXT NOT NULL,
      identity_summary_json TEXT NOT NULL,
      first_seen_at TEXT NOT NULL,
      seen_count_total INTEGER NOT NULL,
      seen_count_run INTEGER NOT NULL,
      is_new_mint INTEGER NOT NULL,
      repeat_label TEXT NOT NULL,
      strict_result TEXT NOT NULL,
      strict_labels_json TEXT NOT NULL,
      paper_minimum_result TEXT,
      paper_minimum_labels_json TEXT NOT NULL,
      blocker_labels_json TEXT NOT NULL,
      paper_decision_id TEXT,
      trade_id TEXT,
      position_id TEXT,
      quote_state TEXT NOT NULL,
      fill_state TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE (run_id, mint_address)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_live_candidate_observations_mint_observed
    ON live_candidate_observations (mint_address, observed_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_live_candidate_observations_trade
    ON live_candidate_observations (trade_id) WHERE trade_id IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS price_snapshots (
      id TEXT PRIMARY KEY,
      mint_address TEXT NOT NULL,
      price_sol REAL,
      price_usd REAL,
      volume_h24 REAL,
      liquidity_usd REAL,
      fdv_usd REAL,
      pair_address TEXT,
      dex_id TEXT,
      observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_price_snapshots_mint_observed
    ON price_snapshots (mint_address, observed_at)
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


async def record_candidate_observation(
    path: str | Path,
    *,
    run_id: str,
    mint_address: str,
    source_names: tuple[str, ...] = (),
    source_event_types: tuple[str, ...] = (),
    candidate_mode: str = "unknown",
    symbol: str | None = None,
    name: str | None = None,
    metadata_completeness_state: str | None = None,
    strict_result: str = "not_evaluated",
    strict_labels: tuple[str, ...] = (),
    paper_minimum_result: str | None = None,
    paper_minimum_labels: tuple[str, ...] = (),
    blocker_labels: tuple[str, ...] = (),
    quote_state: str = "not_requested",
    fill_state: str = "not_traded",
    observed_at: datetime | None = None,
) -> CandidateObservation:
    """Persist one sanitized candidate observation without invoking any trade path."""

    normalized_run_id = run_id.strip()
    normalized_mint = mint_address.strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    if not normalized_mint:
        raise ValueError("mint_address is required")

    timestamp = observed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    timestamp_text = timestamp.astimezone(UTC).isoformat()
    normalized_sources = _normalized_values(source_names)
    normalized_events = _normalized_values(source_event_types)
    identity_summary = json.dumps(
        {
            "symbol": _summary_text(symbol),
            "name": _summary_text(name),
            "source_count": len(normalized_sources),
            "metadata_completeness_state": _summary_text(metadata_completeness_state),
        },
        sort_keys=True,
    )
    strict_labels_json = json.dumps(_normalized_values(strict_labels))
    paper_labels_json = json.dumps(_normalized_values(paper_minimum_labels))
    blocker_labels_json = json.dumps(_normalized_values(blocker_labels))
    normalized_mode = _allowed_value(candidate_mode, {"launch", "migration", "unknown"}, "unknown")
    normalized_strict = _allowed_value(
        strict_result,
        {"passed", "rejected", "unknown", "not_evaluated"},
        "unknown",
    )
    normalized_paper_minimum = (
        _allowed_value(
            paper_minimum_result,
            {"eligible", "blocked", "not_applicable", "not_evaluated"},
            "not_evaluated",
        )
        if paper_minimum_result is not None
        else None
    )
    normalized_quote_state = _allowed_value(
        quote_state,
        {"not_requested", "available", "unavailable", "invalid"},
        "not_requested",
    )
    normalized_fill_state = _allowed_value(
        fill_state,
        {"not_traded", "priced_quote", "unpriced", "position_unavailable"},
        "not_traded",
    )

    async with aiosqlite.connect(path) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            """
            SELECT id, first_seen_at, seen_count_total, seen_count_run,
                   source_names_json, source_event_types_json
            FROM live_candidate_observations
            WHERE run_id = ? AND mint_address = ?
            """,
            (normalized_run_id, normalized_mint),
        )
        same_run = await cursor.fetchone()
        await cursor.close()
        if same_run is not None:
            (
                observation_id,
                first_seen_at,
                total_count,
                run_count,
                existing_sources,
                existing_events,
            ) = same_run
            merged_sources = _merged_values(existing_sources, normalized_sources)
            merged_events = _merged_values(existing_events, normalized_events)
            await db.execute(
                """
                UPDATE live_candidate_observations
                SET observed_at = ?, source_names_json = ?, source_event_types_json = ?,
                    candidate_mode = ?, identity_summary_json = ?, seen_count_total = ?,
                    seen_count_run = ?, is_new_mint = ?, repeat_label = ?, strict_result = ?,
                    strict_labels_json = ?,
                    paper_minimum_result = ?, paper_minimum_labels_json = ?, blocker_labels_json = ?,
                    quote_state = ?, fill_state = ?
                WHERE id = ?
                """,
                (
                    timestamp_text,
                    json.dumps(merged_sources),
                    json.dumps(merged_events),
                    normalized_mode,
                    identity_summary,
                    int(total_count) + 1,
                    int(run_count) + 1,
                    0,
                    "duplicate_same_run_collapsed",
                    normalized_strict,
                    strict_labels_json,
                    normalized_paper_minimum,
                    paper_labels_json,
                    blocker_labels_json,
                    normalized_quote_state,
                    normalized_fill_state,
                    observation_id,
                ),
            )
            await db.commit()
            return CandidateObservation(
                id=str(observation_id),
                run_id=normalized_run_id,
                observed_at=timestamp_text,
                mint_address=normalized_mint,
                first_seen_at=str(first_seen_at),
                seen_count_total=int(total_count) + 1,
                seen_count_run=int(run_count) + 1,
                is_new_mint=False,
                repeat_label="duplicate_same_run_collapsed",
            )

        cursor = await db.execute(
            """
            SELECT first_seen_at, MAX(seen_count_total)
            FROM live_candidate_observations
            WHERE mint_address = ?
            """,
            (normalized_mint,),
        )
        prior = await cursor.fetchone()
        await cursor.close()
        first_seen_at = timestamp_text if prior is None or prior[0] is None else str(prior[0])
        total_count = 1 if prior is None or prior[1] is None else int(prior[1]) + 1
        is_new_mint = prior is None or prior[1] is None
        repeat_label = "new_mint" if is_new_mint else "repeat_prior_run"
        observation_id = str(uuid4())
        await db.execute(
            """
            INSERT INTO live_candidate_observations VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                observation_id,
                normalized_run_id,
                timestamp_text,
                normalized_mint,
                json.dumps(normalized_sources),
                json.dumps(normalized_events),
                normalized_mode,
                identity_summary,
                first_seen_at,
                total_count,
                1,
                1 if is_new_mint else 0,
                repeat_label,
                normalized_strict,
                strict_labels_json,
                normalized_paper_minimum,
                paper_labels_json,
                blocker_labels_json,
                None,
                None,
                None,
                normalized_quote_state,
                normalized_fill_state,
                timestamp_text,
            ),
        )
        await db.commit()
    return CandidateObservation(
        id=observation_id,
        run_id=normalized_run_id,
        observed_at=timestamp_text,
        mint_address=normalized_mint,
        first_seen_at=first_seen_at,
        seen_count_total=total_count,
        seen_count_run=1,
        is_new_mint=is_new_mint,
        repeat_label=repeat_label,
    )


def _normalized_values(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted({value.strip()[:120] for value in values if isinstance(value, str) and value.strip()}))


def _merged_values(existing_json: str, values: tuple[str, ...]) -> tuple[str, ...]:
    try:
        existing = json.loads(existing_json)
    except (TypeError, json.JSONDecodeError):
        existing = []
    existing_values = (
        tuple(value for value in existing if isinstance(value, str))
        if isinstance(existing, list)
        else ()
    )
    return _normalized_values((*existing_values, *values))


def _summary_text(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())[:120]
    return normalized or None


def _allowed_value(value: str | None, allowed: set[str], fallback: str) -> str:
    return value if isinstance(value, str) and value in allowed else fallback


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


async def record_price_snapshot(
    path: str | Path,
    *,
    mint_address: str,
    price_sol: float | None,
    price_usd: float | None,
    volume_h24: float | None,
    liquidity_usd: float | None,
    fdv_usd: float | None,
    pair_address: str | None,
    dex_id: str | None,
) -> None:
    from uuid import uuid4

    async with aiosqlite.connect(path) as db:
        await db.execute(
            """
            INSERT INTO price_snapshots (id, mint_address, price_sol, price_usd,
                                         volume_h24, liquidity_usd, fdv_usd,
                                         pair_address, dex_id, observed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                mint_address,
                price_sol,
                price_usd,
                volume_h24,
                liquidity_usd,
                fdv_usd,
                pair_address,
                dex_id,
                datetime.now(UTC).isoformat(),
            ),
        )
        await db.commit()


async def get_distinct_mints(path: str | Path) -> list[str]:
    async with aiosqlite.connect(path) as db:
        cursor = await db.execute(
            """
            SELECT DISTINCT mint_address FROM (
                SELECT mint_address FROM trades
                UNION
                SELECT mint_address FROM positions
                UNION
                SELECT mint_address FROM paper_decisions
                UNION
                SELECT mint_address FROM live_candidate_observations
            ) ORDER BY mint_address
            """
        )
        rows = await cursor.fetchall()
    return [row[0] for row in rows]
