"""Diagnostic micro-live readiness gate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Mapping

from src.core.config import Settings
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_execution_config import evaluate_live_execution_config
from src.execution.live_guardrails import evaluate_live_guardrails
from src.execution.live_preflight import (
    SupportsTransactionSimulation,
    SupportsWalletBalanceLookup,
    evaluate_live_preflight,
)
from src.execution.position_reconciliation import (
    PositionReconciliationReport,
    SupportsWalletHoldingsLookup,
    reconcile_positions,
)
from src.monitoring.health import HealthStatus, check_health
from src.strategy.position_manager import PositionManager


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    name: str
    ok: bool
    diagnostics: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MicroLiveReadinessReport:
    ready: bool
    checks: tuple[ReadinessCheck, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "checks": [
                {"name": check.name, "ok": check.ok, "diagnostics": list(check.diagnostics)}
                for check in self.checks
            ],
        }

    def lines(self) -> list[str]:
        lines = [f"micro_live_ready={'READY' if self.ready else 'NOT READY'}"]
        for check in self.checks:
            state = "ok" if check.ok else "not_ready"
            diagnostics = ",".join(check.diagnostics) if check.diagnostics else "none"
            lines.append(f"{check.name}={state} diagnostics={diagnostics}")
        return lines


async def evaluate_micro_live_readiness(
    settings: Settings,
    *,
    env: Mapping[str, str] | None = None,
    requested_trade_sol: float | None = None,
    wallet_balance_lookup: SupportsWalletBalanceLookup | None = None,
    transaction_simulator: SupportsTransactionSimulation | None = None,
    position_manager: PositionManager | None = None,
    wallet_holdings_lookup: SupportsWalletHoldingsLookup | None = None,
    circuit_breaker: LiveCircuitBreaker | None = None,
    health_status: HealthStatus | None = None,
) -> MicroLiveReadinessReport:
    trade_size = requested_trade_sol if requested_trade_sol is not None else settings.live_guardrails.max_trade_sol
    checks: list[ReadinessCheck] = []

    guardrails = evaluate_live_guardrails(settings, requested_trade_sol=trade_size, env=env)
    checks.append(ReadinessCheck("guardrails", guardrails.allowed, guardrails.diagnostics))

    execution_config = evaluate_live_execution_config(settings, env=env)
    checks.append(ReadinessCheck("execution_config", execution_config.allowed, execution_config.diagnostics))

    preflight = await evaluate_live_preflight(
        settings,
        requested_trade_sol=trade_size,
        transaction="readiness-check",
        wallet_balance_lookup=wallet_balance_lookup,
        transaction_simulator=transaction_simulator,
    )
    checks.append(ReadinessCheck("preflight", preflight.allowed, preflight.diagnostics))

    if position_manager is None or wallet_holdings_lookup is None:
        reconciliation = PositionReconciliationReport(
            ok=False,
            diagnostics=("position_reconciliation_unavailable",),
            mismatches=(),
        )
    else:
        reconciliation = await reconcile_positions(position_manager, wallet_holdings_lookup)
    checks.append(ReadinessCheck("position_reconciliation", reconciliation.ok, reconciliation.diagnostics))

    if circuit_breaker is None:
        checks.append(ReadinessCheck("circuit_breaker", False, ("circuit_breaker_unavailable",)))
    else:
        breaker_decision = circuit_breaker.status(execution_mode=settings.execution.mode, observed_at=datetime.now(UTC))
        checks.append(ReadinessCheck("circuit_breaker", breaker_decision.allowed, breaker_decision.diagnostics))

    resolved_health = health_status or check_health()
    health_diagnostics = ("health_check_ok",) if resolved_health.ok else ("health_check_failed",)
    checks.append(ReadinessCheck("health", resolved_health.ok, health_diagnostics))

    ready = all(check.ok for check in checks)
    return MicroLiveReadinessReport(ready=ready, checks=tuple(checks))
