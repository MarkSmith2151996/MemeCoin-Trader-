"""Exit trigger helpers for partials, stops, and emergency closes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from src.core.config import ExitConfig, Settings
from src.core.models import CheckResult, PartialExit, Position, RiskAssessment


DEFAULT_TP_LEVELS = (
    (2.0, 0.25),
    (5.0, 0.25),
    (10.0, 0.25),
    (20.0, 0.25),
)


@dataclass(frozen=True)
class ExitAction:
    reason: str
    sell_pct: float
    is_full_exit: bool = False


def build_partial_exits(config: ExitConfig) -> list[PartialExit]:
    if config.tp_levels:
        return [PartialExit(multiple=level.multiple, sell_pct=level.sell_pct) for level in config.tp_levels]
    return [PartialExit(multiple=multiple, sell_pct=sell_pct) for multiple, sell_pct in DEFAULT_TP_LEVELS]


def current_multiple(position: Position, current_price_sol: float) -> float:
    if position.entry_price_sol <= 0:
        return 0.0
    return current_price_sol / position.entry_price_sol


def evaluate_exits(
    position: Position,
    current_price: float,
    pool_liquidity_sol: float | None,
    config: Settings,
    risk_assessment: RiskAssessment | None = None,
) -> list[ExitAction]:
    if current_price <= 0:
        return []

    multiple = current_multiple(position, current_price)
    pnl_pct = ((current_price - position.entry_price_sol) / position.entry_price_sol) if position.entry_price_sol else 0.0
    actions: list[ExitAction] = []

    if multiple < 2.0 and _minutes_open(position) >= config.exits.time_stop_minutes:
        actions.append(ExitAction("time stop: no 2x within configured window", position.remaining_sell_pct, True))

    if pnl_pct <= -config.exits.stop_loss_pct:
        actions.append(ExitAction("hard stop loss triggered", position.remaining_sell_pct, True))

    if pool_liquidity_sol is not None and pool_liquidity_sol < config.risk.min_liquidity_sol:
        actions.append(ExitAction("liquidity dropped below entry threshold", position.remaining_sell_pct, True))

    if risk_assessment is not None:
        if risk_assessment.mint_authority_check == CheckResult.FAIL:
            actions.append(ExitAction("emergency exit: mint authority re-enabled", position.remaining_sell_pct, True))
        if risk_assessment.freeze_authority_check == CheckResult.FAIL:
            actions.append(ExitAction("emergency exit: freeze authority re-enabled", position.remaining_sell_pct, True))
        if risk_assessment.honeypot_check == CheckResult.FAIL:
            actions.append(ExitAction("emergency exit: honeypot signal detected", position.remaining_sell_pct, True))
        if risk_assessment.creator_holding_check == CheckResult.FAIL:
            actions.append(ExitAction("emergency exit: creator selling concentration", position.remaining_sell_pct, True))

    if any(action.is_full_exit for action in actions):
        return [_full_exit(position, actions[0].reason)]

    remaining_pct = position.remaining_sell_pct
    for partial_exit in position.partial_exits:
        if partial_exit.executed or multiple < partial_exit.multiple:
            continue
        is_final_level = partial_exit.multiple >= _highest_multiple(position)
        sell_pct = remaining_pct if is_final_level else min(partial_exit.sell_pct, remaining_pct)
        reason = f"take profit hit {partial_exit.multiple:.1f}x entry"
        actions.append(ExitAction(reason, sell_pct, is_final_level))
        remaining_pct = max(remaining_pct - sell_pct, 0.0)

    return actions


def _minutes_open(position: Position) -> float:
    return max((datetime.now(UTC) - position.opened_at).total_seconds() / 60, 0.0)


def _highest_multiple(position: Position) -> float:
    configured = [partial_exit.multiple for partial_exit in position.partial_exits]
    return max(configured, default=0.0)


def _full_exit(position: Position, reason: str) -> ExitAction:
    return ExitAction(reason, position.remaining_sell_pct, True)
