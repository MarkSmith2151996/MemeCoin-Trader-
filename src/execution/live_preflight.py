"""Fail-closed live preflight helpers for future swap submission."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.core.config import Settings


class SupportsWalletBalanceLookup(Protocol):
    async def __call__(self) -> float | None: ...


class SupportsTransactionSimulation(Protocol):
    async def __call__(self, transaction: str | bytes) -> object: ...


@dataclass(frozen=True, slots=True)
class TransactionSimulationResult:
    ok: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class LivePreflightDecision:
    allowed: bool
    diagnostics: tuple[str, ...]
    wallet_balance_sol: float | None
    minimum_balance_sol: float


async def evaluate_live_preflight(
    settings: Settings,
    *,
    requested_trade_sol: float | None,
    transaction: str | bytes,
    wallet_balance_lookup: SupportsWalletBalanceLookup | None,
    transaction_simulator: SupportsTransactionSimulation | None,
) -> LivePreflightDecision:
    diagnostics: list[str] = []
    minimum_balance_sol = settings.live_guardrails.min_wallet_balance_sol

    wallet_balance_sol: float | None = None
    if wallet_balance_lookup is None:
        diagnostics.append("wallet_balance_lookup_unavailable")
    else:
        try:
            wallet_balance_sol = await wallet_balance_lookup()
        except Exception:
            diagnostics.append("wallet_balance_lookup_failed")
        else:
            if wallet_balance_sol is None:
                diagnostics.append("wallet_balance_unknown")
            elif requested_trade_sol is None:
                diagnostics.append("requested_trade_sol_missing")
            elif wallet_balance_sol < requested_trade_sol + minimum_balance_sol:
                diagnostics.append("insufficient_wallet_balance")

    if transaction_simulator is None:
        diagnostics.append("transaction_simulator_unavailable")
    else:
        try:
            simulation_result = _normalize_simulation_result(await transaction_simulator(transaction))
        except Exception:
            diagnostics.append("transaction_simulation_failed_provider")
        else:
            if not simulation_result.ok:
                diagnostics.append("transaction_simulation_failed")

    if diagnostics:
        return LivePreflightDecision(
            allowed=False,
            diagnostics=tuple(diagnostics),
            wallet_balance_sol=wallet_balance_sol,
            minimum_balance_sol=minimum_balance_sol,
        )

    return LivePreflightDecision(
        allowed=True,
        diagnostics=("live_preflight_passed",),
        wallet_balance_sol=wallet_balance_sol,
        minimum_balance_sol=minimum_balance_sol,
    )


def _normalize_simulation_result(value: object) -> TransactionSimulationResult:
    if isinstance(value, TransactionSimulationResult):
        return value
    if isinstance(value, bool):
        return TransactionSimulationResult(ok=value, error=None if value else "simulation returned false")
    if isinstance(value, dict):
        ok_value = value.get("ok")
        if isinstance(ok_value, bool):
            error = value.get("error") if isinstance(value.get("error"), str) else None
            return TransactionSimulationResult(ok=ok_value, error=error)
        error_value = value.get("error")
        if error_value:
            return TransactionSimulationResult(ok=False, error=str(error_value))
    return TransactionSimulationResult(ok=False, error="unrecognized simulation result")
