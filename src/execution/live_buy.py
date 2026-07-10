"""Guarded micro-live buy helper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.core.config import Settings
from src.core.models import Side, Signal, SignalSource, SignalType, Trade
from src.execution.jupiter_live import JupiterLiveExecutionAdapter
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_preflight import SupportsTransactionSimulation, SupportsWalletBalanceLookup
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.execution.position_reconciliation import SupportsWalletHoldingsLookup
from src.strategy.position_manager import PositionManager


class SupportsBuyTransactionBuilder(Protocol):
    async def __call__(self, mint_address: str, amount_sol: float) -> str | bytes | None: ...


@dataclass(frozen=True, slots=True)
class LiveBuyResult:
    ok: bool
    diagnostics: tuple[str, ...]
    tx_signature: str | None = None
    provider: str | None = None


async def execute_guarded_live_buy(
    *,
    settings: Settings,
    mint_address: str,
    amount_sol: float,
    position_manager: PositionManager,
    adapter: JupiterLiveExecutionAdapter,
    buy_transaction_builder: SupportsBuyTransactionBuilder | None,
    wallet_holdings_lookup: SupportsWalletHoldingsLookup | None,
    wallet_balance_lookup: SupportsWalletBalanceLookup | None,
    transaction_simulator: SupportsTransactionSimulation | None,
    circuit_breaker: LiveCircuitBreaker | None,
    env: dict[str, str] | None = None,
) -> LiveBuyResult:
    if buy_transaction_builder is None:
        return LiveBuyResult(ok=False, diagnostics=("buy_transaction_builder_unavailable",))

    if amount_sol <= 0:
        return LiveBuyResult(ok=False, diagnostics=("requested_trade_sol_invalid",))

    existing = await position_manager.get_position(mint_address, mode="live")
    if existing is not None:
        return LiveBuyResult(ok=False, diagnostics=("open_position_exists",))

    readiness = await evaluate_micro_live_readiness(
        settings,
        env=env,
        requested_trade_sol=amount_sol,
        wallet_balance_lookup=wallet_balance_lookup,
        transaction_simulator=transaction_simulator,
        position_manager=position_manager,
        wallet_holdings_lookup=wallet_holdings_lookup,
        circuit_breaker=circuit_breaker,
    )
    if not readiness.ready:
        return LiveBuyResult(
            ok=False,
            diagnostics=tuple(f"readiness:{check.name}" for check in readiness.checks if not check.ok),
        )

    serialized_tx = await buy_transaction_builder(mint_address, amount_sol)
    if not serialized_tx:
        return LiveBuyResult(ok=False, diagnostics=("buy_transaction_build_failed",))

    submission = await adapter.submit_serialized_swap(serialized_tx, amount_sol=amount_sol)
    if not submission.ok:
        return LiveBuyResult(
            ok=False,
            diagnostics=tuple(submission.diagnostics) if submission.diagnostics else ("live_buy_submission_failed",),
            provider=submission.provider,
        )

    trade = Trade(
        mint_address=mint_address,
        side=Side.BUY,
        amount_sol=amount_sol,
        token_amount=amount_sol,
        price_sol=1.0,
        tx_signature=submission.tx_signature,
        mode="live",
        status="submitted",
        metadata={"provider": submission.provider or "unknown", "guarded_micro_live": True},
    )
    signal = Signal(
        source=SignalSource.MANUAL,
        type=SignalType.BUY,
        mint_address=mint_address,
        confidence=1.0,
        payload={"guarded_micro_live": True},
    )
    await position_manager.open_position(trade, signal)

    return LiveBuyResult(
        ok=True,
        diagnostics=("live_buy_submitted",),
        tx_signature=submission.tx_signature,
        provider=submission.provider,
    )
