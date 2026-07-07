"""Helpers for liquidity-based position size caps."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Sequence


@dataclass(frozen=True, slots=True)
class LiquiditySizingTier:
    min_liquidity_sol: float = 0.0
    max_liquidity_sol: float | None = None
    max_position_sol: float = 0.0
    skip_trade: bool = False
    label: str = "custom"

    def matches(self, pool_liquidity_sol: float) -> bool:
        if pool_liquidity_sol < self.min_liquidity_sol:
            return False
        return self.max_liquidity_sol is None or pool_liquidity_sol < self.max_liquidity_sol


@dataclass(frozen=True, slots=True)
class LiquiditySizingDecision:
    max_position_sol: float
    matched_tier: str | None
    skip_trade: bool
    reason: str


DEFAULT_LIQUIDITY_SIZING_TIERS: tuple[LiquiditySizingTier, ...] = (
    LiquiditySizingTier(max_liquidity_sol=15.0, max_position_sol=0.1, label="under_15_sol"),
    LiquiditySizingTier(min_liquidity_sol=15.0, max_liquidity_sol=50.0, max_position_sol=0.25, label="15_to_50_sol"),
    LiquiditySizingTier(min_liquidity_sol=50.0, max_liquidity_sol=200.0, max_position_sol=0.5, label="50_to_200_sol"),
    LiquiditySizingTier(min_liquidity_sol=200.0, max_position_sol=1.0, label="over_200_sol"),
)


def determine_liquidity_position_size(
    pool_liquidity_sol: float | None,
    tiers: Sequence[LiquiditySizingTier] | None = None,
) -> LiquiditySizingDecision:
    configured_tiers = tuple(tiers or DEFAULT_LIQUIDITY_SIZING_TIERS)
    if pool_liquidity_sol is None:
        return LiquiditySizingDecision(
            max_position_sol=0.0,
            matched_tier=None,
            skip_trade=True,
            reason="liquidity_unknown",
        )

    liquidity_value = float(pool_liquidity_sol)
    if not isfinite(liquidity_value) or liquidity_value < 0:
        return LiquiditySizingDecision(
            max_position_sol=0.0,
            matched_tier=None,
            skip_trade=True,
            reason="liquidity_invalid",
        )

    for tier in configured_tiers:
        if tier.matches(liquidity_value):
            return LiquiditySizingDecision(
                max_position_sol=round(max(tier.max_position_sol, 0.0), 6),
                matched_tier=tier.label,
                skip_trade=tier.skip_trade,
                reason=tier.label,
            )

    return LiquiditySizingDecision(
        max_position_sol=0.0,
        matched_tier=None,
        skip_trade=True,
        reason="liquidity_unmatched",
    )
