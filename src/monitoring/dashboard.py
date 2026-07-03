"""Dashboard data helpers."""

from __future__ import annotations

from src.core.models import Position
from src.strategy.portfolio import open_exposure_sol


def summarize_positions(positions: list[Position]) -> dict[str, float | int]:
    return {
        "open_count": len(positions),
        "open_exposure_sol": open_exposure_sol(positions),
    }
