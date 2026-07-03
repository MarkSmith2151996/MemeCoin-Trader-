"""Portfolio cap checks."""

from __future__ import annotations

from src.core.config import PositionConfig
from src.core.models import Position, PositionStatus


def open_exposure_sol(positions: list[Position]) -> float:
    return sum(position.amount_sol for position in positions if position.status != PositionStatus.CLOSED)


def can_open_position(positions: list[Position], amount_sol: float, config: PositionConfig) -> bool:
    open_positions = [position for position in positions if position.status != PositionStatus.CLOSED]
    if amount_sol > config.max_single_position_sol:
        return False
    if len(open_positions) >= config.max_open_positions:
        return False
    return open_exposure_sol(open_positions) + amount_sol <= config.max_portfolio_sol
