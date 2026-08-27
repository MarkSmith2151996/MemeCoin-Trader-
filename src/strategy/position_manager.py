"""Async helpers for position lifecycle and persistence."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from src.core.config import Settings
from src.core.database import record_position
from src.core.models import PaperFillQuality, PartialExit, Position, PositionStatus, Signal, Trade
from src.strategy.exits import build_partial_exits

# ── Round-trip execution cost model (MT-584) ─────────────────────────
# Estimated execution costs deducted from raw realized PnL to produce the
# adjusted_pnl_sol column on closed positions. Per-leg values for calm
# conditions, tuned via these module-level constants.
PRIORITY_FEE_PER_LEG = 0.0002  # SOL priority fee per leg
DEX_FEE_PCT = 0.01             # 1% bonding-curve fee per leg
SLIPPAGE_PCT = 0.00187         # 0.187% measured median per leg (MT-594, 233 matched trades Aug 6-8)


def _estimated_round_trip_cost_sol(position: Position) -> float:
    """Estimated round-trip execution cost in SOL for a position.

    Cost = priority fee (per leg) + dex fee (per leg, % of entry size)
           + slippage (per leg, % of entry size), both legs counted.
    """
    entry_sol = max(position.amount_sol, 0.0)
    return (
        PRIORITY_FEE_PER_LEG * 2
        + DEX_FEE_PCT * entry_sol * 2
        + SLIPPAGE_PCT * entry_sol * 2
    )


class PositionManager:
    def __init__(
        self,
        db: str | Path | None,
        config: Settings,
        *,
        use_persisted_positions: bool = True,
        persist_positions: bool = True,
        strategy: str = "A",
    ) -> None:
        self.db = Path(db) if db is not None else None
        self.config = config
        self.use_persisted_positions = use_persisted_positions
        self.persist_positions = persist_positions
        self.strategy = strategy
        self._cache: dict[tuple[str, str], Position] = {}

    async def open_position(self, trade: Trade, signal: Signal) -> Position:
        known_price = trade.price_sol if trade.price_sol is not None else self._signal_price(signal)
        if known_price is not None and not _is_valid_price(known_price):
            raise ValueError(f"Invalid entry price for position: {known_price}")
        entry_price = known_price if _is_valid_price(known_price) else 0.0
        token_amount = trade.token_amount
        if token_amount is not None and not _is_valid_quantity(token_amount):
            raise ValueError(f"Invalid token amount for position: {token_amount}")
        if token_amount is None:
            token_amount = trade.amount_sol / entry_price if entry_price > 0 else 0.0
        fill_quality = (
            PaperFillQuality.PRICED_QUOTE
            if entry_price > 0 and token_amount > 0
            else PaperFillQuality.UNPRICED
        )
        position = Position(
            mint_address=trade.mint_address,
            entry_trade_id=trade.id,
            amount_sol=trade.amount_sol,
            token_amount=token_amount,
            entry_price_sol=entry_price,
            mode=trade.mode or "paper",
            fill_quality=fill_quality,
            partial_exits=build_partial_exits(self.config.exits),
        )
        self._cache[(position.mint_address, position.mode)] = position
        await self._persist(position)
        return position

    async def get_position(self, mint: str, *, mode: str | None = None) -> Position | None:
        cached = self._cached_position(mint, mode)
        if cached is not None and cached.status != PositionStatus.CLOSED and not cached.archived:
            return cached

        if not self.use_persisted_positions:
            return None

        position = await self._fetch_position(mint, mode=mode)
        if position is not None:
            self._cache[(position.mint_address, position.mode)] = position
        if position is None or position.status == PositionStatus.CLOSED or position.archived:
            return None
        return position

    async def get_all_open(
        self,
        *,
        include_archived: bool = False,
        mode: str | None = None,
    ) -> list[Position]:
        if self.db is None or not self.use_persisted_positions:
            return [
                position
                for position in self._cache.values()
                if position.status != PositionStatus.CLOSED
                and (include_archived or not position.archived)
                and (mode is None or position.mode == mode)
            ]

        async with aiosqlite.connect(self.db) as conn:
            query = "SELECT partial_exits_json FROM positions WHERE status != ? AND strategy = ?"
            params: list[str] = [PositionStatus.CLOSED.value, self.strategy]
            if mode is not None:
                query += " AND mode = ?"
                params.append(mode)
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()

        positions = [Position.model_validate_json(row[0]) for row in rows]
        self._cache.update({(position.mint_address, position.mode): position for position in positions})
        if include_archived:
            return [position for position in positions if mode is None or position.mode == mode]
        return [
            position
            for position in positions
            if not position.archived and (mode is None or position.mode == mode)
        ]

    async def record_partial_exit(
        self,
        mint: str,
        exit: PartialExit,
        realized_pnl_sol: float = 0.0,
        *,
        mode: str | None = None,
    ) -> None:
        position = await self.get_position(mint, mode=mode)
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
        self._cache[(updated.mint_address, updated.mode)] = updated
        await self._persist(updated)

    async def close_position(
        self,
        mint: str,
        exit_price_sol: float | None = None,
        *,
        mode: str | None = None,
        peak_price_sol: float | None = None,
    ) -> Position | None:
        position = self._cached_position(mint, mode)
        if position is None:
            position = await self._fetch_position(mint, mode=mode)
        if position is None:
            return None

        realized_pnl = 0.0
        close_price = exit_price_sol
        if exit_price_sol is not None and (not math.isfinite(exit_price_sol) or exit_price_sol < 0):
            raise ValueError(f"Invalid exit price for position: {exit_price_sol}")
        if exit_price_sol is not None:
            remaining_fraction = position.remaining_sell_pct
            realized_pnl = round(
                position.remaining_token_amount * exit_price_sol
                - position.amount_sol * remaining_fraction,
                9,
            )

        closed = position.model_copy(
            update={
                "status": PositionStatus.CLOSED,
                "closed_at": datetime.now(UTC),
                "realized_pnl_sol": round(position.realized_pnl_sol + realized_pnl, 9),
                # MT-584: raw PnL minus estimated round-trip execution costs.
                "adjusted_pnl_sol": round(
                    position.realized_pnl_sol + realized_pnl - _estimated_round_trip_cost_sol(position),
                    9,
                ),
                "close_price_sol": close_price,
                "peak_price_sol": (
                    peak_price_sol if peak_price_sol is not None else position.peak_price_sol
                ),
            }
        )
        self._cache[(closed.mint_address, closed.mode)] = closed
        await self._persist(closed)
        return closed

    async def update_peak_price(
        self,
        mint: str,
        peak_price_sol: float,
        *,
        mode: str | None = None,
    ) -> Position | None:
        """Persist the highest observed mark for an open position."""
        if not _is_valid_price(peak_price_sol):
            raise ValueError(f"Invalid peak price for position: {peak_price_sol}")
        position = await self.get_position(mint, mode=mode)
        if position is None:
            return None
        previous_peak = position.peak_price_sol
        if previous_peak is not None and peak_price_sol < previous_peak:
            return position
        updated = position.model_copy(update={"peak_price_sol": peak_price_sol})
        self._cache[(updated.mint_address, updated.mode)] = updated
        await self._persist(updated)
        return updated

    async def get_paper_positions(self, *, include_archived: bool = False) -> list[Position]:
        """Return all open positions with mode == 'paper'."""
        return await self.get_all_open(include_archived=include_archived, mode="paper")

    async def get_archived_paper_positions(self) -> list[Position]:
        """Return archived paper positions excluded from default reports."""
        all_positions = await self.get_all_open(include_archived=True, mode="paper")
        return [p for p in all_positions if p.archived]

    async def get_legacy_paper_positions(self) -> list[Position]:
        """Return active legacy paper positions that are safe archive candidates."""
        paper_positions = await self.get_paper_positions()
        return [p for p in paper_positions if p.fill_quality == PaperFillQuality.LEGACY_UNKNOWN]

    async def archive_legacy_paper_positions(self) -> int:
        """Archive active legacy paper positions without touching live or priced rows."""
        legacy_positions = await self.get_legacy_paper_positions()
        archived_at = datetime.now(UTC)
        for position in legacy_positions:
            archived = position.model_copy(
                update={
                    "archived": True,
                    "archived_at": archived_at,
                    "archive_reason": "legacy_fill_quality",
                }
            )
            self._cache[(archived.mint_address, archived.mode)] = archived
            await self._persist(archived)
        return len(legacy_positions)

    async def close_paper_positions(self) -> int:
        """Close all open paper positions. Returns count closed. Never touches live positions."""
        paper_positions = await self.get_paper_positions()
        for position in paper_positions:
            await self.close_position(position.mint_address, mode="paper")
        return len(paper_positions)

    async def total_exposure_sol(self, *, mode: str | None = None) -> float:
        positions = await self.get_all_open(mode=mode)
        return round(sum(position.amount_sol * position.remaining_sell_pct for position in positions), 6)

    def _cached_position(self, mint: str, mode: str | None) -> Position | None:
        if mode is not None:
            return self._cache.get((mint, mode))
        return next((position for (cached_mint, _), position in self._cache.items() if cached_mint == mint), None)

    async def _fetch_position(self, mint: str, *, mode: str | None = None) -> Position | None:
        if self.db is None or not self.use_persisted_positions:
            return self._cached_position(mint, mode)

        async with aiosqlite.connect(self.db) as conn:
            query = """SELECT partial_exits_json FROM positions
                       WHERE mint_address = ? AND status != ? AND strategy = ?"""
            params: list[str] = [mint, PositionStatus.CLOSED.value, self.strategy]
            if mode is not None:
                query += " AND mode = ?"
                params.append(mode)
            query += " ORDER BY opened_at DESC"
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        positions = [Position.model_validate_json(row[0]) for row in rows]
        return next((position for position in positions if mode is None or position.mode == mode), None)

    async def _persist(self, position: Position) -> None:
        if self.db is None or not self.persist_positions:
            return
        await record_position(self.db, position, strategy=self.strategy)

    @staticmethod
    def _signal_price(signal: Signal | None) -> float | None:
        if signal is None:
            return None
        raw_price = signal.payload.get("price_sol")
        if isinstance(raw_price, (int, float)) and _is_valid_price(float(raw_price)):
            return float(raw_price)
        return None


def _is_valid_price(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


def _is_valid_quantity(value: float) -> bool:
    return math.isfinite(value) and value >= 0
