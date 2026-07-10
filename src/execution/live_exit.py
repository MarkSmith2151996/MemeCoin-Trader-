"""Guarded sell-only live exit helper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from src.core.config import Settings
from src.execution.jupiter_live import JupiterLiveExecutionAdapter, LiveSubmissionResult
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_preflight import SupportsTransactionSimulation, SupportsWalletBalanceLookup
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.execution.position_reconciliation import SupportsWalletHoldingsLookup
from src.strategy.position_manager import PositionManager


class SupportsExitTransactionBuilder(Protocol):
    async def __call__(self, mint_address: str, amount_sol: float) -> str | bytes | None: ...


@dataclass(frozen=True, slots=True)
class LiveExitResult:
    ok: bool
    diagnostics: tuple[str, ...]
    tx_signature: str | None = None
    provider: str | None = None


async def execute_guarded_live_exit(
    *,
    settings: Settings,
    mint_address: str,
    position_manager: PositionManager,
    adapter: JupiterLiveExecutionAdapter,
    exit_transaction_builder: SupportsExitTransactionBuilder | None,
    wallet_holdings_lookup: SupportsWalletHoldingsLookup | None,
    wallet_balance_lookup: SupportsWalletBalanceLookup | None,
    transaction_simulator: SupportsTransactionSimulation | None,
    circuit_breaker: LiveCircuitBreaker | None,
    env: dict[str, str] | None = None,
) -> LiveExitResult:
    if exit_transaction_builder is None:
        return LiveExitResult(ok=False, diagnostics=("exit_transaction_builder_unavailable",))

    readiness = await evaluate_micro_live_readiness(
        settings,
        env=env,
        requested_trade_sol=settings.live_guardrails.max_trade_sol,
        wallet_balance_lookup=wallet_balance_lookup,
        transaction_simulator=transaction_simulator,
        position_manager=position_manager,
        wallet_holdings_lookup=wallet_holdings_lookup,
        circuit_breaker=circuit_breaker,
    )
    if not readiness.ready:
        return LiveExitResult(
            ok=False,
            diagnostics=tuple(f"readiness:{check.name}" for check in readiness.checks if not check.ok),
        )

    position = await position_manager.get_position(mint_address)
    if position is None:
        return LiveExitResult(ok=False, diagnostics=("position_not_found",))

    amount_sol = max(position.amount_sol * position.remaining_sell_pct, 0.000001)
    serialized_tx = await exit_transaction_builder(mint_address, amount_sol)
    if not serialized_tx:
        return LiveExitResult(ok=False, diagnostics=("exit_transaction_build_failed",))

    submission = await adapter.submit_serialized_swap(serialized_tx, amount_sol=amount_sol)
    if not submission.ok:
        return LiveExitResult(
            ok=False,
            diagnostics=tuple(submission.diagnostics) if submission.diagnostics else ("live_exit_submission_failed",),
            provider=submission.provider,
        )

    await position_manager.close_position(mint_address)
    return LiveExitResult(
        ok=True,
        diagnostics=("live_exit_submitted",),
        tx_signature=submission.tx_signature,
        provider=submission.provider,
    )
