"""Minimal Jito block-engine submission scaffold.

This module is intentionally pre-live only: it builds bundle submission payloads and
supports injectable HTTP submission for tests, but it does not sign transactions,
manage wallets, or enable live trading on its own.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from src.execution.redaction import sanitize_provider_error


DEFAULT_JITO_ENDPOINT = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"


class SupportsPost(Protocol):
    async def post(self, url: str, json: dict[str, Any]) -> Any: ...


@dataclass(slots=True)
class JitoBundleRequest:
    endpoint: str
    transactions: list[str]
    payload: dict[str, Any]
    tip_lamports: int | None = None


@dataclass(slots=True)
class JitoSubmitResult:
    ok: bool
    bundle_id: str | None
    error: str | None
    used_endpoint: str
    tip_lamports: int | None
    status_code: int | None = None


class JitoBlockEngineClient:
    def __init__(
        self,
        endpoint: str | None = None,
        *,
        timeout_s: float = 10.0,
        http_client: SupportsPost | None = None,
    ) -> None:
        self.endpoint = (endpoint or DEFAULT_JITO_ENDPOINT).rstrip("/")
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)

    def build_bundle_request(
        self,
        transactions: list[str | bytes],
        *,
        tip_lamports: int | None = None,
        validator_tip_account: str | None = None,
        encoding: str = "base64",
    ) -> JitoBundleRequest:
        normalized_transactions = [self._normalize_transaction(tx) for tx in transactions]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [normalized_transactions],
        }
        metadata: dict[str, Any] = {"encoding": encoding}
        if tip_lamports is not None:
            metadata["tipLamports"] = tip_lamports
        if validator_tip_account:
            metadata["validatorTipAccount"] = validator_tip_account
        if metadata:
            payload["params"].append(metadata)

        return JitoBundleRequest(
            endpoint=self.endpoint,
            transactions=normalized_transactions,
            payload=payload,
            tip_lamports=tip_lamports,
        )

    async def submit_bundle(
        self,
        transactions: list[str | bytes],
        *,
        tip_lamports: int | None = None,
        validator_tip_account: str | None = None,
    ) -> JitoSubmitResult:
        """Fail closed for direct callers; guarded adapters use the private transport path."""
        request = self.build_bundle_request(
            transactions,
            tip_lamports=tip_lamports,
            validator_tip_account=validator_tip_account,
        )
        return self._failure("direct_jito_submission_blocked", request)

    async def _submit_bundle_for_guarded_adapter(
        self,
        transactions: list[str | bytes],
        *,
        tip_lamports: int | None = None,
        validator_tip_account: str | None = None,
    ) -> JitoSubmitResult:
        """Transport-only path for JupiterLiveExecutionAdapter after safety checks."""
        request = self.build_bundle_request(
            transactions,
            tip_lamports=tip_lamports,
            validator_tip_account=validator_tip_account,
        )

        try:
            response = await self._client.post(request.endpoint, json=request.payload)
        except httpx.TimeoutException:
            return self._failure("request timed out", request)
        except Exception as exc:
            return self._failure(f"provider exception: {sanitize_provider_error(exc)}", request)

        status_code = getattr(response, "status_code", None)
        if status_code != 200:
            return self._failure(f"unexpected status: {status_code}", request, status_code=status_code)

        try:
            body = response.json()
        except Exception:
            return self._failure("malformed json response", request, status_code=status_code)

        if not isinstance(body, dict):
            return self._failure("malformed response body", request, status_code=status_code)

        bundle_id = self._extract_bundle_id(body)
        if not bundle_id:
            return self._failure("missing bundle id", request, status_code=status_code)

        return JitoSubmitResult(
            ok=True,
            bundle_id=bundle_id,
            error=None,
            used_endpoint=request.endpoint,
            tip_lamports=request.tip_lamports,
            status_code=status_code,
        )

    async def close(self) -> None:
        if self._owns_client and hasattr(self._client, "aclose"):
            await self._client.aclose()

    def _normalize_transaction(self, transaction: str | bytes) -> str:
        if isinstance(transaction, str):
            return transaction
        return base64.b64encode(transaction).decode("ascii")

    def _extract_bundle_id(self, body: dict[str, Any]) -> str | None:
        result = body.get("result")
        if isinstance(result, str) and result:
            return result
        if isinstance(result, dict):
            for key in ("bundle_id", "bundleId", "id"):
                value = result.get(key)
                if isinstance(value, str) and value:
                    return value
        return None

    def _failure(
        self,
        error: str,
        request: JitoBundleRequest,
        *,
        status_code: int | None = None,
    ) -> JitoSubmitResult:
        return JitoSubmitResult(
            ok=False,
            bundle_id=None,
            error=error,
            used_endpoint=request.endpoint,
            tip_lamports=request.tip_lamports,
            status_code=status_code,
        )
