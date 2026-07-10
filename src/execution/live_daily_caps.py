"""Fail-closed daily live submission cap checks."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DailyLiveState:
    """Received or persisted live activity totals for one UTC day."""

    day: date
    submitted_trade_count: int
    realized_loss_sol: float


@dataclass(frozen=True, slots=True)
class DailyLiveCapDecision:
    allowed: bool
    diagnostics: tuple[str, ...]


def evaluate_daily_live_caps(
    state: DailyLiveState | None,
    *,
    max_daily_trades: int | None,
    max_daily_loss_sol: float | None,
    today: date | None = None,
) -> DailyLiveCapDecision:
    """Allow only a current, well-formed activity state below both limits."""
    if state is None:
        return DailyLiveCapDecision(False, ("daily_live_state_unavailable",))

    if state.day != (today or date.today()):
        return DailyLiveCapDecision(False, ("daily_live_state_stale",))

    if (
        isinstance(state.submitted_trade_count, bool)
        or not isinstance(state.submitted_trade_count, int)
        or state.submitted_trade_count < 0
        or not isinstance(state.realized_loss_sol, (int, float))
        or not math.isfinite(state.realized_loss_sol)
        or state.realized_loss_sol < 0
    ):
        return DailyLiveCapDecision(False, ("daily_live_state_malformed",))

    if max_daily_trades is None or max_daily_loss_sol is None:
        return DailyLiveCapDecision(False, ("daily_live_cap_config_unavailable",))
    if state.submitted_trade_count >= max_daily_trades:
        return DailyLiveCapDecision(False, ("daily_live_trade_cap_exhausted",))
    if state.realized_loss_sol >= max_daily_loss_sol:
        return DailyLiveCapDecision(False, ("daily_live_loss_cap_exhausted",))
    return DailyLiveCapDecision(True, ("daily_live_caps_passed",))
