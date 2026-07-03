"""Position lifecycle helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from src.core.models import Position, PositionStatus, Trade


def open_position(entry_trade: Trade) -> Position:
    price = entry_trade.price_sol or 1.0
    token_amount = entry_trade.token_amount or entry_trade.amount_sol / price
    return Position(
        mint_address=entry_trade.mint_address,
        entry_trade_id=entry_trade.id,
        amount_sol=entry_trade.amount_sol,
        token_amount=token_amount,
        entry_price_sol=price,
    )


def close_position(position: Position, realized_pnl_sol: float = 0.0) -> Position:
    return position.model_copy(
        update={
            "status": PositionStatus.CLOSED,
            "closed_at": datetime.now(UTC),
            "realized_pnl_sol": realized_pnl_sol,
        }
    )
