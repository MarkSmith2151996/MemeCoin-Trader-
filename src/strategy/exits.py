"""Exit trigger helpers for partials, stops, and emergency closes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from src.core.config import ExitConfig, Settings
from src.core.models import CheckResult, PartialExit, Position, RiskAssessment
from src.strategy.dynamic_exits import evaluate_liquidity_emergency, evaluate_trail_start, evaluate_volume_decay


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
    reason_labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class DynamicExitState:
    current_volume: float | None = None
    peak_volume: float | None = None
    volume_below_threshold_started_at: datetime | None = None
    reference_liquidity: float | None = None
    reference_liquidity_at: datetime | None = None
    observed_at: datetime | None = None


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
    dynamic_state: DynamicExitState | None = None,
) -> list[ExitAction]:
    if current_price <= 0:
        return []

    multiple = current_multiple(position, current_price)
    pnl_pct = ((current_price - position.entry_price_sol) / position.entry_price_sol) if position.entry_price_sol else 0.0
    actions: list[ExitAction] = []

    if config.exits.dynamic_exits_enabled and dynamic_state is not None:
        actions.extend(_dynamic_exit_actions(position, multiple, pool_liquidity_sol, config.exits, dynamic_state))

    if multiple < 2.0 and _minutes_open(position) >= config.exits.time_stop_minutes:
        actions.append(
            ExitAction(
                "time stop: no 2x within configured window",
                position.remaining_sell_pct,
                True,
                ("time_stop_exit",),
            )
        )

    if pnl_pct <= -config.exits.stop_loss_pct:
        actions.append(ExitAction("hard stop loss triggered", position.remaining_sell_pct, True, ("hard_stop_loss",)))

    if pool_liquidity_sol is not None and pool_liquidity_sol < config.risk.min_liquidity_sol:
        actions.append(
            ExitAction(
                "liquidity dropped below entry threshold",
                position.remaining_sell_pct,
                True,
                ("min_liquidity_breach",),
            )
        )

    if risk_assessment is not None:
        if risk_assessment.mint_authority_check == CheckResult.FAIL:
            actions.append(
                ExitAction(
                    "emergency exit: mint authority re-enabled",
                    position.remaining_sell_pct,
                    True,
                    ("mint_authority_emergency",),
                )
            )
        if risk_assessment.freeze_authority_check == CheckResult.FAIL:
            actions.append(
                ExitAction(
                    "emergency exit: freeze authority re-enabled",
                    position.remaining_sell_pct,
                    True,
                    ("freeze_authority_emergency",),
                )
            )
        if risk_assessment.honeypot_check == CheckResult.FAIL:
            actions.append(
                ExitAction(
                    "emergency exit: honeypot signal detected",
                    position.remaining_sell_pct,
                    True,
                    ("honeypot_emergency",),
                )
            )
        if risk_assessment.creator_holding_check == CheckResult.FAIL:
            actions.append(
                ExitAction(
                    "emergency exit: creator selling concentration",
                    position.remaining_sell_pct,
                    True,
                    ("creator_concentration_emergency",),
                )
            )

    if any(action.is_full_exit for action in actions):
        return [_full_exit(position, actions[0].reason, actions[0].reason_labels)]

    remaining_pct = position.remaining_sell_pct
    for partial_exit in position.partial_exits:
        if partial_exit.executed or multiple < partial_exit.multiple:
            continue
        is_final_level = partial_exit.multiple >= _highest_multiple(position)
        sell_pct = remaining_pct if is_final_level else min(partial_exit.sell_pct, remaining_pct)
        reason = f"take profit hit {partial_exit.multiple:.1f}x entry"
        actions.append(ExitAction(reason, sell_pct, is_final_level, ("take_profit",)))
        remaining_pct = max(remaining_pct - sell_pct, 0.0)

    return actions


def _dynamic_exit_actions(
    position: Position,
    multiple: float,
    pool_liquidity_sol: float | None,
    config: ExitConfig,
    dynamic_state: DynamicExitState,
) -> list[ExitAction]:
    actions: list[ExitAction] = []
    observed_at = dynamic_state.observed_at or datetime.now(UTC)

    if dynamic_state.current_volume is not None and dynamic_state.peak_volume is not None:
        volume_decay = evaluate_volume_decay(
            current_volume=dynamic_state.current_volume,
            peak_volume=dynamic_state.peak_volume,
            below_threshold_started_at=dynamic_state.volume_below_threshold_started_at,
            observed_at=observed_at,
            threshold_ratio=config.dynamic_volume_decay_ratio,
            min_duration=timedelta(minutes=config.dynamic_volume_decay_minutes),
        )
        if volume_decay.triggered:
            actions.append(
                ExitAction(
                    "dynamic exit: volume decayed below calibrated threshold",
                    position.remaining_sell_pct,
                    True,
                    volume_decay.reason_labels,
                )
            )

    if pool_liquidity_sol is not None and dynamic_state.reference_liquidity is not None and dynamic_state.reference_liquidity_at is not None:
        liquidity_emergency = evaluate_liquidity_emergency(
            current_liquidity=pool_liquidity_sol,
            reference_liquidity=dynamic_state.reference_liquidity,
            reference_at=dynamic_state.reference_liquidity_at,
            observed_at=observed_at,
            drop_ratio_threshold=config.dynamic_liquidity_drop_ratio,
            max_window=timedelta(seconds=config.dynamic_liquidity_drop_window_seconds),
        )
        if liquidity_emergency.triggered:
            actions.append(
                ExitAction(
                    "dynamic exit: liquidity collapsed inside emergency window",
                    position.remaining_sell_pct,
                    True,
                    liquidity_emergency.reason_labels,
                )
            )

    if config.dynamic_trail_start_multiple is not None:
        trail_start = evaluate_trail_start(
            current_multiple=multiple,
            trail_start_multiple=config.dynamic_trail_start_multiple,
        )
        if trail_start.triggered:
            actions.append(
                ExitAction(
                    "dynamic signal: trailing stop can arm early",
                    0.0,
                    False,
                    trail_start.reason_labels,
                )
            )

    return actions


def _minutes_open(position: Position) -> float:
    return max((datetime.now(UTC) - position.opened_at).total_seconds() / 60, 0.0)


def _highest_multiple(position: Position) -> float:
    configured = [partial_exit.multiple for partial_exit in position.partial_exits]
    return max(configured, default=0.0)


def _full_exit(position: Position, reason: str, reason_labels: tuple[str, ...] = ()) -> ExitAction:
    return ExitAction(reason, position.remaining_sell_pct, True, reason_labels)
