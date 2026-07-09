"""Closed-by-default live trading guardrails."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from src.core.config import Settings


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


@dataclass(frozen=True, slots=True)
class LiveGuardrailDecision:
    allowed: bool
    diagnostics: tuple[str, ...]
    max_trade_sol: float | None
    max_daily_trades: int | None
    max_daily_loss_sol: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "diagnostics": list(self.diagnostics),
            "max_trade_sol": self.max_trade_sol,
            "max_daily_trades": self.max_daily_trades,
            "max_daily_loss_sol": self.max_daily_loss_sol,
        }


def evaluate_live_guardrails(
    settings: Settings,
    *,
    requested_trade_sol: float | None = None,
    env: Mapping[str, str] | None = None,
) -> LiveGuardrailDecision:
    resolved_env = env if env is not None else os.environ
    diagnostics: list[str] = []

    max_trade_sol = _env_float(resolved_env, "MAX_LIVE_TRADE_SOL", settings.live_guardrails.max_trade_sol)
    max_daily_trades = _env_int(resolved_env, "MAX_LIVE_DAILY_TRADES", settings.live_guardrails.max_daily_trades)
    max_daily_loss_sol = _env_float(
        resolved_env,
        "MAX_LIVE_DAILY_LOSS_SOL",
        settings.live_guardrails.max_daily_loss_sol,
    )

    if settings.execution.mode != "live":
        diagnostics.append("execution_mode_not_live")

    if _env_bool(resolved_env.get("LIVE_TRADING_ENABLED")) is not True:
        diagnostics.append("live_trading_env_not_enabled")

    if resolved_env.get("LIVE_CONFIRMATION_PHRASE", "") != settings.live_guardrails.confirmation_phrase:
        diagnostics.append("live_confirmation_phrase_invalid")

    if _env_bool(resolved_env.get("LIVE_KILL_SWITCH")) is not False:
        diagnostics.append("live_kill_switch_not_explicitly_false")

    if max_trade_sol is None or max_trade_sol <= 0:
        diagnostics.append("max_live_trade_sol_invalid")
    if max_daily_trades is None or max_daily_trades <= 0:
        diagnostics.append("max_live_daily_trades_invalid")
    if max_daily_loss_sol is None or max_daily_loss_sol <= 0:
        diagnostics.append("max_live_daily_loss_sol_invalid")

    if requested_trade_sol is None:
        diagnostics.append("requested_trade_sol_missing")
    elif max_trade_sol is not None and requested_trade_sol > max_trade_sol:
        diagnostics.append("requested_trade_sol_exceeds_max_live_trade_sol")

    if diagnostics:
        return LiveGuardrailDecision(
            allowed=False,
            diagnostics=tuple(diagnostics),
            max_trade_sol=max_trade_sol,
            max_daily_trades=max_daily_trades,
            max_daily_loss_sol=max_daily_loss_sol,
        )

    return LiveGuardrailDecision(
        allowed=True,
        diagnostics=("live_guardrails_passed",),
        max_trade_sol=max_trade_sol,
        max_daily_trades=max_daily_trades,
        max_daily_loss_sol=max_daily_loss_sol,
    )


def _env_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    return None


def _env_float(env: Mapping[str, str], name: str, default: float) -> float | None:
    raw_value = env.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return float(raw_value)
    except ValueError:
        return None


def _env_int(env: Mapping[str, str], name: str, default: int) -> int | None:
    raw_value = env.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return int(raw_value)
    except ValueError:
        return None
