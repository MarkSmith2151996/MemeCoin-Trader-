"""Live Jupiter execution adapter boundary.

The live adapter remains disabled for real trading, but this module now exposes a
small, injectable submission path so Jito protection can be wired and tested
without requiring wallets, real RPC calls, or signed transactions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from src.chain.jito import JitoBlockEngineClient, JitoSubmitResult
from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter


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
    ) -> None:
        self._jito_enabled = jito_enabled
        self._jito_fallback_to_rpc = jito_fallback_to_rpc
        self._jito_tip_lamports = jito_tip_lamports
        self._jito_validator_tip_account = jito_validator_tip_account
        self._jito_client = jito_client or JitoBlockEngineClient()
        self._rpc_submitter = rpc_submitter

    @property
    def mode(self) -> str:
        return "live"

    async def submit_serialized_swap(self, transaction: str | bytes) -> LiveSubmissionResult:
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

    async def _submit_via_rpc(
        self,
        transaction: str | bytes,
        *,
        diagnostics: list[str],
        jito_result: JitoSubmitResult | None = None,
    ) -> LiveSubmissionResult:
        if self._rpc_submitter is None:
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
            return LiveSubmissionResult(
                ok=False,
                provider="rpc",
                jito_result=jito_result,
                error=f"rpc submission failed: {exc}",
                diagnostics=diagnostics,
            )

        return LiveSubmissionResult(
            ok=True,
            provider="rpc",
            tx_signature=tx_signature,
            jito_result=jito_result,
            diagnostics=diagnostics,
        )
