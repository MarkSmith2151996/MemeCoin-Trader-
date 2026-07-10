"""Helius-backed provider implementations for live-readiness checks.

All providers are read-only — they query state, never sign or submit
transactions. They fail closed to NOT READY when config is missing or
the RPC is unreachable.

Current provider: transaction_simulator.
Future: wallet_balance_lookup, wallet_holdings_lookup.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
from dotenv import dotenv_values

from src.execution.live_preflight import (
    SupportsTransactionSimulation,
    TransactionSimulationResult,
)


def _resolve_helius_rpc_url() -> str:
    """Resolve the Helius RPC URL from env or .env without exposing the key."""
    direct_url = os.getenv("HELIUS_RPC_URL", "").strip()
    if direct_url:
        return direct_url

    direct_key = os.getenv("HELIUS_API_KEY", "").strip()
    if direct_key:
        return f"https://mainnet.helius-rpc.com/?api-key={direct_key}"

    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        dotenv_data = dotenv_values(dotenv_path)
        url_from_dotenv = dotenv_data.get("HELIUS_RPC_URL")
        if isinstance(url_from_dotenv, str) and url_from_dotenv.strip():
            return url_from_dotenv.strip()
        key_from_dotenv = dotenv_data.get("HELIUS_API_KEY")
        if isinstance(key_from_dotenv, str) and key_from_dotenv.strip():
            return f"https://mainnet.helius-rpc.com/?api-key={key_from_dotenv.strip()}"

    return ""


class HeliusTransactionSimulator:
    """SupportsTransactionSimulation backed by Helius RPC.

    For readiness checks (transaction="readiness-check"), performs a
    lightweight getLatestBlockhash call to verify the RPC is reachable.
    For real transactions, calls simulateTransaction.

    Never logs the API key, raw transaction bytes, or full RPC URL.
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        self._rpc_url = (rpc_url if rpc_url is not None else _resolve_helius_rpc_url()).rstrip("/")
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)

    async def _rpc_call(self, method: str, params: list[object] | None = None) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
        response = await self._client.post(self._rpc_url, json=payload)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            msg = str(body["error"])
            raise RuntimeError(msg)
        return body.get("result")

    async def __call__(self, transaction: str | bytes) -> TransactionSimulationResult:
        if not self._rpc_url:
            return TransactionSimulationResult(ok=False, error="helius_rpc_url_not_configured")

        if isinstance(transaction, str) and transaction == "readiness-check":
            try:
                await self._rpc_call("getLatestBlockhash")
                return TransactionSimulationResult(ok=True)
            except Exception as exc:
                return TransactionSimulationResult(ok=False, error=f"helius_rpc_unreachable: {exc}")

        try:
            raw_tx = transaction if isinstance(transaction, bytes) else transaction.encode()
            result = await self._rpc_call("simulateTransaction", [list(raw_tx)])
            if isinstance(result, dict):
                if result.get("err"):
                    return TransactionSimulationResult(
                        ok=False,
                        error=f"simulation_failed: {result['err']}",
                    )
                return TransactionSimulationResult(ok=True)
            return TransactionSimulationResult(ok=False, error="unexpected_simulation_response")
        except Exception as exc:
            return TransactionSimulationResult(ok=False, error=f"simulation_error: {exc}")

    async def close(self) -> None:
        await self._client.aclose()


def try_create_transaction_simulator() -> HeliusTransactionSimulator | None:
    """Create a HeliusTransactionSimulator if Helius RPC config is available.

    Returns None if HELIUS_API_KEY / HELIUS_RPC_URL is not configured,
    so the readiness gate can report transaction_simulator_unavailable
    instead of transaction_simulation_failed.
    """
    rpc_url = _resolve_helius_rpc_url()
    if not rpc_url:
        return None
    return HeliusTransactionSimulator(rpc_url=rpc_url)
