"""Coverage for periodic open-position price snapshots."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.core.database import (
    init_db,
    prune_position_price_snapshots,
    record_position_price_snapshot,
)
from src.core.models import Position
from src.monitoring.position_snapshots import snapshot_open_positions


class FakeManager:
    def __init__(self, positions: list[Position]) -> None:
        self._positions = positions

    async def get_all_open(self, *, mode: str) -> list[Position]:
        assert mode == "paper"
        return self._positions


class FakePriceProvider:
    def __init__(self, prices: dict[str, float | None]) -> None:
        self._prices = prices

    async def get_current_price(self, mint_address: str) -> float | None:
        return self._prices.get(mint_address)


def make_position(position_id: str, mint_address: str) -> Position:
    return Position(
        id=position_id,
        mint_address=mint_address,
        entry_trade_id=f"{position_id}-trade",
        amount_sol=0.01,
        token_amount=1_000,
        entry_price_sol=0.00001,
    )


def test_snapshot_worker_records_valid_open_position_marks(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    manager = FakeManager([make_position("pos-1", "Mint1"), make_position("pos-2", "Mint2")])
    provider = FakePriceProvider({"Mint1": 0.00002, "Mint2": None})

    recorded = asyncio.run(snapshot_open_positions(manager, provider, db_path))

    assert recorded == 1
    with sqlite3.connect(db_path) as db:
        rows = db.execute(
            "SELECT position_id, mint_address, price_sol, timestamp FROM price_snapshots",
        ).fetchall()
        indexes = db.execute("PRAGMA index_list(price_snapshots)").fetchall()
    assert rows[0][:3] == ("pos-1", "Mint1", 0.00002)
    assert rows[0][3]
    assert any(index[1] == "idx_snapshots_position" for index in indexes)


def test_pruning_only_removes_expired_position_snapshots(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    now = datetime.now(UTC)
    asyncio.run(record_position_price_snapshot(
        db_path,
        position_id="expired",
        mint_address="OldMint",
        price_sol=0.00001,
        observed_at=now - timedelta(days=8),
    ))
    with sqlite3.connect(db_path) as db:
        db.execute(
            """INSERT INTO price_snapshots (id, mint_address, price_sol, observed_at, timestamp)
               VALUES ('historical', 'HistoricalMint', 0.00001, ?, ?)""",
            ((now - timedelta(days=8)).isoformat(), (now - timedelta(days=8)).isoformat()),
        )
        db.commit()

    asyncio.run(prune_position_price_snapshots(db_path, now=now))

    with sqlite3.connect(db_path) as db:
        rows = db.execute(
            "SELECT position_id, mint_address FROM price_snapshots ORDER BY mint_address",
        ).fetchall()
    assert rows == [(None, "HistoricalMint")]
