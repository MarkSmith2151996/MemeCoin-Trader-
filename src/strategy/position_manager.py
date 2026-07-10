"""Async helpers for position lifecycle and persistence."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from src.core.config import Settings
from src.core.database import record_position
from src.core.models import PartialExit, Position, PositionStatus, Signal, Trade
from src.strategy.exits import build_partial_exits


class PositionManager:
    def __init__(
        self,
        db: str | Path | None,
        config: Settings,
        *,
        use_persisted_positions: bool = True,
        persist_positions: bool = True,
    ) -> None:
        self.db = Path(db) if db is not None else None
        self.config = config
        self.use_persisted_positions = use_persisted_positions
        self.persist_positions = persist_positions
        self._cache: dict[str, Position] = {}

    async def open_position(self, trade: Trade, signal: Signal) -> Position:
        known_price = trade.price_sol if trade.price_sol is not None else self._signal_price(signal)
        if known_price is not None and (known_price <= 0 or math.isnan(known_price)):
            raise ValueError(f"Invalid entry price for paper position: {known_price}")
        entry_price = known_price if known_price is not None and known_price > 0 else 0.0
        token_amount = trade.token_amount
        if token_amount is None:
            token_amount = trade.amount_sol / entry_price if entry_price > 0 else 0.0
        position = Position(
            mint_address=trade.mint_address,
            entry_trade_id=trade.id,
            amount_sol=trade.amount_sol,
            token_amount=token_amount,
            entry_price_sol=entry_price,
            mode=trade.mode or "paper",
            partial_exits=build_partial_exits(self.config.exits),
        )
        self._cache[position.mint_address] = position
        await self._persist(position)
        return position

    async def get_position(self, mint: str) -> Position | None:
        cached = self._cache.get(mint)
        if cached is not None and cached.status != PositionStatus.CLOSED:
            return cached

        if not self.use_persisted_positions:
            return None

        position = await self._fetch_position(mint)
        if position is not None:
            self._cache[mint] = position
        if position is None or position.status == PositionStatus.CLOSED:
            return None
        return position

    async def get_all_open(self) -> list[Position]:
        if self.db is None or not self.use_persisted_positions:
            return [position for position in self._cache.values() if position.status != PositionStatus.CLOSED]

        async with aiosqlite.connect(self.db) as conn:
            cursor = await conn.execute(
                "SELECT partial_exits_json FROM positions WHERE status != ?",
                (PositionStatus.CLOSED.value,),
            )
            rows = await cursor.fetchall()

        positions = [Position.model_validate_json(row[0]) for row in rows]
        self._cache.update({position.mint_address: position for position in positions})
        return positions

    async def record_partial_exit(
        self,
        mint: str,
        exit: PartialExit,
        realized_pnl_sol: float = 0.0,
    ) -> None:
        position = await self.get_position(mint)
        if position is None:
            return

        partial_exits = list(position.partial_exits)
        replaced = False
        for index, existing in enumerate(partial_exits):
            if not existing.executed and abs(existing.multiple - exit.multiple) < 1e-9:
                partial_exits[index] = exit
                replaced = True
                break
        if not replaced:
            partial_exits.append(exit)

        updated = position.model_copy(
            update={
                "partial_exits": partial_exits,
                "status": PositionStatus.PARTIAL,
                "realized_pnl_sol": round(position.realized_pnl_sol + realized_pnl_sol, 9),
            }
        )
        self._cache[mint] = updated
        await self._persist(updated)

    async def close_position(self, mint: str, exit_price_sol: float | None = None) -> Position | None:
        position = await self._fetch_position(mint) if mint not in self._cache else self._cache[mint]
        if position is None:
            return None

        realized_pnl = 0.0
        close_price = exit_price_sol
        if exit_price_sol is not None and exit_price_sol > 0:
            realized_pnl = round(position.token_amount * exit_price_sol - position.amount_sol, 9)

        closed = position.model_copy(
            update={
                "status": PositionStatus.CLOSED,
                "closed_at": datetime.now(UTC),
                "realized_pnl_sol": round(position.realized_pnl_sol + realized_pnl, 9),
                "close_price_sol": close_price,
            }
        )
        self._cache[mint] = closed
        await self._persist(closed)
        return closed

    async def get_paper_positions(self) -> list[Position]:
        """Return all open positions with mode == 'paper'."""
        all_open = await self.get_all_open()
        return [p for p in all_open if p.mode == "paper"]

    async def close_paper_positions(self) -> int:
        """Close all open paper positions. Returns count closed. Never touches live positions."""
        paper_positions = await self.get_paper_positions()
        for position in paper_positions:
            await self.close_position(position.mint_address)
        return len(paper_positions)

    async def total_exposure_sol(self) -> float:
        positions = await self.get_all_open()
        return round(sum(position.amount_sol * position.remaining_sell_pct for position in positions), 6)

    async def _fetch_position(self, mint: str) -> Position | None:
        if self.db is None or not self.use_persisted_positions:
            return self._cache.get(mint)

        async with aiosqlite.connect(self.db) as conn:
            cursor = await conn.execute(
                "SELECT partial_exits_json FROM positions WHERE mint_address = ? ORDER BY opened_at DESC LIMIT 1",
                (mint,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return Position.model_validate_json(row[0])

    async def _persist(self, position: Position) -> None:
        if self.db is None or not self.persist_positions:
            return
        await record_position(self.db, position)

    @staticmethod
    def _signal_price(signal: Signal | None) -> float | None:
        if signal is None:
            return None
        raw_price = signal.payload.get("price_sol")
        if isinstance(raw_price, (int, float)) and raw_price > 0:
            return float(raw_price)
        return None
