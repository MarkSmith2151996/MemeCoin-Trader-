"""Pure trailing-stop rules for the isolated paper New Pairs momentum lane."""

from __future__ import annotations

from dataclasses import dataclass


HARD_STOP_PCT = -0.25
TRAIL_ACTIVATION_PCT = 0.10
TIGHTENED_TRAIL_ACTIVATION_PCT = 0.50
STANDARD_TRAIL_PCT = 0.10
TIGHTENED_TRAIL_PCT = 0.07


@dataclass(frozen=True, slots=True)
class MomentumTrailState:
    entry_price_sol: float
    peak_price_sol: float
    trail_activated: bool = False


@dataclass(frozen=True, slots=True)
class MomentumTrailDecision:
    state: MomentumTrailState
    exit_reason: str | None = None


def evaluate_momentum_trail(state: MomentumTrailState, mark_price_sol: float) -> MomentumTrailDecision:
    """Update a paper-only peak and return a hard-stop or trailing-stop decision."""

    if state.entry_price_sol <= 0 or mark_price_sol <= 0:
        return MomentumTrailDecision(state)
    entry_change = (mark_price_sol - state.entry_price_sol) / state.entry_price_sol
    if entry_change <= HARD_STOP_PCT:
        return MomentumTrailDecision(state, "hard_stop_loss")

    peak = max(state.peak_price_sol, mark_price_sol)
    peak_change = (peak - state.entry_price_sol) / state.entry_price_sol
    activated = state.trail_activated or peak_change >= TRAIL_ACTIVATION_PCT
    updated = MomentumTrailState(state.entry_price_sol, peak, activated)
    if not activated:
        return MomentumTrailDecision(updated)

    trail_pct = TIGHTENED_TRAIL_PCT if peak_change >= TIGHTENED_TRAIL_ACTIVATION_PCT else STANDARD_TRAIL_PCT
    if (peak - mark_price_sol) / peak >= trail_pct - 1e-12:
        return MomentumTrailDecision(updated, "trailing_stop_tight" if trail_pct == TIGHTENED_TRAIL_PCT else "trailing_stop")
    return MomentumTrailDecision(updated)
