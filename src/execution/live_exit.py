"""Guarded sell-only live exit helper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from src.core.config import Settings
from src.core.database import record_trade
from src.core.models import Side, Trade
from src.execution.jupiter_live import JupiterLiveExecutionAdapter, LiveSubmissionResult
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_preflight import SupportsTransactionSimulation, SupportsWalletBalanceLookup
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.execution.position_reconciliation import SupportsWalletHoldingsLookup
from src.strategy.position_manager import PositionManager


class SupportsExitTransactionBuilder(Protocol):
    async def __call__(self, mint_address: str, token_amount: float) -> str | bytes | None: ...


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
    if adapter.mode != "live":
        return LiveExitResult(ok=False, diagnostics=("live_exit_adapter_mode_invalid",))
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
        allow_exit_while_breaker_tripped=True,
    )
    if not readiness.ready:
        return LiveExitResult(
            ok=False,
            diagnostics=tuple(f"readiness:{check.name}" for check in readiness.checks if not check.ok),
        )

    position = await position_manager.get_position(mint_address, mode="live")
    if position is None:
        return LiveExitResult(ok=False, diagnostics=("position_not_found",))

    amount_sol = max(position.amount_sol * position.remaining_sell_pct, 0.000001)
    token_amount = position.remaining_token_amount
    serialized_tx = await exit_transaction_builder(mint_address, token_amount)
    if not serialized_tx:
        return LiveExitResult(ok=False, diagnostics=("exit_transaction_build_failed",))

    submission = await adapter.submit_serialized_swap(
        serialized_tx,
        amount_sol=amount_sol,
        allow_tripped_circuit_breaker=True,
    )
    if not submission.ok:
        return LiveExitResult(
            ok=False,
            diagnostics=tuple(submission.diagnostics) if submission.diagnostics else ("live_exit_submission_failed",),
            provider=submission.provider,
        )

    try:
        holdings_after = await wallet_holdings_lookup() if wallet_holdings_lookup is not None else None
    except Exception:
        holdings_after = None
    if holdings_after is None:
        return LiveExitResult(
            ok=False,
            diagnostics=("live_exit_wallet_confirmation_unavailable",),
            tx_signature=submission.tx_signature,
            provider=submission.provider,
        )
    remaining_balance = holdings_after.get(mint_address, 0.0)
    if remaining_balance > 0:
        return LiveExitResult(
            ok=False,
            diagnostics=("live_exit_wallet_balance_not_cleared",),
            tx_signature=submission.tx_signature,
            provider=submission.provider,
        )

    trade = Trade(
        mint_address=mint_address,
        side=Side.SELL,
        amount_sol=0,
        token_amount=token_amount,
        price_sol=0,
        tx_signature=submission.tx_signature,
        mode="live",
        status="wallet_confirmed",
        metadata={
            "provider": submission.provider,
            "guarded_micro_live": True,
            "fill_price_unavailable": True,
        },
    )
    if position_manager.db is None:
        return LiveExitResult(
            ok=False,
            diagnostics=("live_exit_trade_persistence_unavailable",),
            tx_signature=submission.tx_signature,
            provider=submission.provider,
        )
    await record_trade(position_manager.db, trade)
    await position_manager.close_position(mint_address, exit_price_sol=0, mode="live")
    return LiveExitResult(
        ok=True,
        diagnostics=("live_exit_wallet_confirmed",),
        tx_signature=submission.tx_signature,
        provider=submission.provider,
    )
