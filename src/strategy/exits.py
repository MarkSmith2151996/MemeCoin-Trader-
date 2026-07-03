"""Exit trigger helpers."""

from __future__ import annotations

from src.core.config import ExitConfig
from src.core.models import PartialExit, Position


def build_partial_exits(config: ExitConfig) -> list[PartialExit]:
    return [PartialExit(multiple=level.multiple, sell_pct=level.sell_pct) for level in config.tp_levels]


def current_multiple(position: Position, current_price_sol: float) -> float:
    if position.entry_price_sol <= 0:
        return 0.0
    return current_price_sol / position.entry_price_sol


def exits_due(position: Position, current_price_sol: float) -> list[PartialExit]:
    multiple = current_multiple(position, current_price_sol)
    return [exit for exit in position.partial_exits if not exit.executed and multiple >= exit.multiple]
