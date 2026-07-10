"""Live Jupiter execution adapter boundary.

The live adapter remains disabled for real trading, but this module now exposes a
small, injectable submission path so Jito protection can be wired and tested
without requiring wallets, real RPC calls, or signed transactions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from src.chain.jito import JitoBlockEngineClient, JitoSubmitResult
from src.core.config import Settings, load_settings
from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_execution_config import LiveExecutionConfigDecision, evaluate_live_execution_config
from src.execution.live_guardrails import LiveGuardrailDecision, evaluate_live_guardrails
from src.execution.live_preflight import (
    LivePreflightDecision,
    SupportsTransactionSimulation,
    SupportsWalletBalanceLookup,
    evaluate_live_preflight,
)


class SupportsRpcSubmit(Protocol):
    async def __call__(self, transaction: str | bytes) -> str: ...


@dataclass(slots=True)
class LiveSubmissionResult:
    ok: bool
    provider: str
    tx_signature: str | None = None
    jito_result: JitoSubmitResult | None = None
    error: str | None = None
    diagnostics: list[str] = field(default_factory=list)


class JupiterLiveExecutionAdapter(ExecutionAdapter):
    def __init__(
        self,
        *,
        jito_enabled: bool = False,
        jito_fallback_to_rpc: bool = True,
        jito_tip_lamports: int | None = None,
        jito_validator_tip_account: str | None = None,
        jito_client: JitoBlockEngineClient | None = None,
        rpc_submitter: SupportsRpcSubmit | None = None,
        backup_rpc_submitter: SupportsRpcSubmit | None = None,
        settings: Settings | None = None,
        guardrail_env: dict[str, str] | None = None,
        wallet_balance_lookup: SupportsWalletBalanceLookup | None = None,
        transaction_simulator: SupportsTransactionSimulation | None = None,
        circuit_breaker: LiveCircuitBreaker | None = None,
    ) -> None:
        self._jito_enabled = jito_enabled
        self._jito_fallback_to_rpc = jito_fallback_to_rpc
        self._jito_tip_lamports = jito_tip_lamports
        self._jito_validator_tip_account = jito_validator_tip_account
        self._jito_client = jito_client or JitoBlockEngineClient()
        self._rpc_submitter = rpc_submitter
        self._backup_rpc_submitter = backup_rpc_submitter
        self._settings = settings or load_settings()
        self._guardrail_env = guardrail_env
        self._wallet_balance_lookup = wallet_balance_lookup
        self._transaction_simulator = transaction_simulator
        self._circuit_breaker = circuit_breaker

        if self._jito_tip_lamports is None:
            self._jito_tip_lamports = self._settings.execution.jito_tip_lamports

    @property
    def mode(self) -> str:
        return "live"

    async def submit_serialized_swap(
        self,
        transaction: str | bytes,
        *,
        amount_sol: float | None = None,
    ) -> LiveSubmissionResult:
        if self._circuit_breaker is not None:
            breaker_decision = self._circuit_breaker.status(execution_mode=self._settings.execution.mode)
            if not breaker_decision.allowed:
                return LiveSubmissionResult(
                    ok=False,
                    provider="circuit_breaker",
                    error="live circuit breaker blocked submission",
                    diagnostics=list(breaker_decision.diagnostics),
                )

        guardrails = self.live_guardrails(amount_sol)
        if not guardrails.allowed:
            return LiveSubmissionResult(
                ok=False,
                provider="guardrails",
                error="live guardrails blocked submission",
                diagnostics=list(guardrails.diagnostics),
            )

        preflight = await self.live_preflight(transaction=transaction, amount_sol=amount_sol)
        if not preflight.allowed:
            self._record_preflight_failure(preflight.diagnostics)
            return LiveSubmissionResult(
                ok=False,
                provider="preflight",
                error="live preflight blocked submission",
                diagnostics=list(preflight.diagnostics),
            )
        self._record_preflight_success()

        execution_config = self.live_execution_config()
        if not execution_config.allowed:
            return LiveSubmissionResult(
                ok=False,
                provider="config",
                error="live execution config invalid",
                diagnostics=list(execution_config.diagnostics),
            )

        diagnostics: list[str] = []

        if self._jito_enabled:
            diagnostics.append("jito_attempted")
            jito_result = await self._jito_client.submit_bundle(
                [transaction],
                tip_lamports=self._jito_tip_lamports,
                validator_tip_account=self._jito_validator_tip_account,
            )
            if jito_result.ok:
                diagnostics.append("jito_bundle_submitted")
                self._record_submission_success()
                return LiveSubmissionResult(
                    ok=True,
                    provider="jito",
                    jito_result=jito_result,
                    diagnostics=diagnostics,
                )

            if self._jito_fallback_to_rpc:
                diagnostics.append("jito_failed_fallback_rpc")
                return await self._submit_via_rpc(transaction, diagnostics=diagnostics, jito_result=jito_result)

            diagnostics.append("jito_failed_no_fallback")
            self._record_submission_failure()
            return LiveSubmissionResult(
                ok=False,
                provider="jito",
                jito_result=jito_result,
                error=jito_result.error,
                diagnostics=diagnostics,
            )

        diagnostics.append("jito_disabled")
        return await self._submit_via_rpc(transaction, diagnostics=diagnostics)

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        raise NotImplementedError("Live swaps must be implemented behind risk gates")

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        raise NotImplementedError("Live Jupiter quotes are a Phase 2 task")

    async def get_current_price(self, mint_address: str) -> float | None:
        raise NotImplementedError("Live price lookup is a Phase 2 task")

    async def close(self) -> None:
        await self._jito_client.close()
        return None

    def live_guardrails(self, requested_trade_sol: float | None = None) -> LiveGuardrailDecision:
        return evaluate_live_guardrails(
            self._settings,
            requested_trade_sol=requested_trade_sol,
            env=self._guardrail_env,
        )

    def live_execution_config(self) -> LiveExecutionConfigDecision:
        return evaluate_live_execution_config(self._settings, env=self._guardrail_env)

    async def live_preflight(
        self,
        *,
        transaction: str | bytes,
        amount_sol: float | None,
    ) -> LivePreflightDecision:
        return await evaluate_live_preflight(
            self._settings,
            requested_trade_sol=amount_sol,
            transaction=transaction,
            wallet_balance_lookup=self._wallet_balance_lookup,
            transaction_simulator=self._transaction_simulator,
        )

    async def _submit_via_rpc(
        self,
        transaction: str | bytes,
        *,
        diagnostics: list[str],
        jito_result: JitoSubmitResult | None = None,
    ) -> LiveSubmissionResult:
        if self._rpc_submitter is None:
            self._record_rpc_failure()
            return LiveSubmissionResult(
                ok=False,
                provider="rpc",
                jito_result=jito_result,
                error="rpc submitter not configured",
                diagnostics=diagnostics,
            )

        try:
            tx_signature = await self._rpc_submitter(transaction)
        except Exception as exc:
            self._record_rpc_failure()
            if self._backup_rpc_submitter is not None:
                try:
                    tx_signature = await self._backup_rpc_submitter(transaction)
                except Exception as backup_exc:
                    self._record_submission_failure()
                    return LiveSubmissionResult(
                        ok=False,
                        provider="rpc",
                        jito_result=jito_result,
                        error=f"rpc submission failed: {exc}; backup failed: {backup_exc}",
                        diagnostics=[*diagnostics, "rpc_primary_failed_backup_failed"],
                    )

                self._record_submission_success()
                return LiveSubmissionResult(
                    ok=True,
                    provider="rpc_backup",
                    tx_signature=tx_signature,
                    jito_result=jito_result,
                    diagnostics=[*diagnostics, "rpc_primary_failed_backup_used"],
                )

            self._record_submission_failure()
            return LiveSubmissionResult(
                ok=False,
                provider="rpc",
                jito_result=jito_result,
                error=f"rpc submission failed: {exc}",
                diagnostics=diagnostics,
            )

        self._record_rpc_success()
        self._record_submission_success()
        return LiveSubmissionResult(
            ok=True,
            provider="rpc",
            tx_signature=tx_signature,
            jito_result=jito_result,
            diagnostics=diagnostics,
        )

    def _record_preflight_failure(self, diagnostics: tuple[str, ...]) -> None:
        if self._circuit_breaker is None:
            return
        if any(reason.startswith("transaction_simulation_") for reason in diagnostics):
            self._circuit_breaker.record_simulation_failure()

    def _record_preflight_success(self) -> None:
        if self._circuit_breaker is None:
            return
        self._circuit_breaker.record_simulation_success()

    def _record_rpc_failure(self) -> None:
        if self._circuit_breaker is None:
            return
        self._circuit_breaker.record_rpc_failure()

    def _record_rpc_success(self) -> None:
        if self._circuit_breaker is None:
            return
        self._circuit_breaker.record_rpc_success()

    def _record_submission_failure(self) -> None:
        if self._circuit_breaker is None:
            return
        self._circuit_breaker.record_submission_failure()

    def _record_submission_success(self) -> None:
        if self._circuit_breaker is None:
            return
        self._circuit_breaker.record_submission_success()
