"""Helius-backed provider implementations for live-readiness checks.

All providers are read-only — they query state, never sign or submit
transactions. They fail closed to NOT READY when config is missing or
the RPC is unreachable.

Current providers:
  - transaction_simulator (HeliusTransactionSimulator)
  - wallet_balance_lookup (HeliusWalletBalanceLookup)
  - wallet_holdings_lookup (HeliusWalletHoldingsLookup)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
from dotenv import dotenv_values

from src.execution.live_preflight import (
    SupportsTransactionSimulation,
    SupportsWalletBalanceLookup,
    TransactionSimulationResult,
)
from src.execution.position_reconciliation import SupportsWalletHoldingsLookup


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


def _resolve_trading_wallet_public_key() -> str:
    """Resolve the read-only wallet public key from env or .env."""
    direct = os.getenv("TRADING_WALLET_PUBLIC_KEY", "").strip()
    if direct:
        return direct

    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        dotenv_data = dotenv_values(dotenv_path)
        key_from_dotenv = dotenv_data.get("TRADING_WALLET_PUBLIC_KEY")
        if isinstance(key_from_dotenv, str) and key_from_dotenv.strip():
            return key_from_dotenv.strip()

    return ""


LAMPORTS_PER_SOL: float = 1_000_000_000


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


class HeliusWalletBalanceLookup:
    """SupportsWalletBalanceLookup backed by Helius RPC.

    Calls getBalance for the configured public key.
    Never logs the API key, wallet address, or full RPC URL.
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        wallet_public_key: str | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        self._rpc_url = (rpc_url if rpc_url is not None else _resolve_helius_rpc_url()).rstrip("/")
        self._public_key = wallet_public_key if wallet_public_key is not None else _resolve_trading_wallet_public_key()
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)

    async def __call__(self) -> float | None:
        if not self._rpc_url:
            return None
        if not self._public_key:
            return None

        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [self._public_key]}
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
            body = response.json()
        except Exception:
            return None

        if not isinstance(body, dict):
            return None
        if "error" in body:
            return None
        result = body.get("result")
        if not isinstance(result, dict):
            return None
        lamports = result.get("value")
        if not isinstance(lamports, int):
            return None
        return lamports / LAMPORTS_PER_SOL

    async def close(self) -> None:
        await self._client.aclose()


def try_create_balance_lookup() -> HeliusWalletBalanceLookup | None:
    """Create a HeliusWalletBalanceLookup if Helius RPC and wallet public key are available.

    Returns None if either is missing, so the readiness gate can report
    wallet_balance_lookup_unavailable instead of wallet_balance_unknown.
    """
    rpc_url = _resolve_helius_rpc_url()
    public_key = _resolve_trading_wallet_public_key()
    if not rpc_url or not public_key:
        return None
    return HeliusWalletBalanceLookup(rpc_url=rpc_url, wallet_public_key=public_key)


class HeliusWalletHoldingsLookup:
    """SupportsWalletHoldingsLookup backed by Helius RPC.

    Calls getTokenAccountsByOwner for the configured public key and
    returns a dict of mint_address -> token_amount for non-zero balances.
    Never logs the API key, wallet address, or full RPC URL.
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        wallet_public_key: str | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        self._rpc_url = (rpc_url if rpc_url is not None else _resolve_helius_rpc_url()).rstrip("/")
        self._public_key = wallet_public_key if wallet_public_key is not None else _resolve_trading_wallet_public_key()
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)

    async def __call__(self) -> dict[str, float] | None:
        if not self._rpc_url:
            return None
        if not self._public_key:
            return None

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                self._public_key,
                {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                {"encoding": "jsonParsed"},
            ],
        }
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
            body = response.json()
        except Exception:
            return None

        if not isinstance(body, dict):
            return None
        if "error" in body:
            return None
        result = body.get("result")
        if not isinstance(result, dict):
            return None
        accounts = result.get("value")
        if not isinstance(accounts, list):
            return None

        holdings: dict[str, float] = {}
        for account in accounts:
            if not isinstance(account, dict):
                continue
            account_data = account.get("account")
            if not isinstance(account_data, dict):
                continue
            data = account_data.get("data")
            if not isinstance(data, dict):
                continue
            parsed = data.get("parsed")
            if not isinstance(parsed, dict):
                continue
            info = parsed.get("info")
            if not isinstance(info, dict):
                continue
            mint = info.get("mint")
            token_amount = info.get("tokenAmount")
            if not isinstance(mint, str) or not isinstance(token_amount, dict):
                continue
            ui_amount = token_amount.get("uiAmount")
            if isinstance(ui_amount, (int, float)) and ui_amount > 0:
                holdings[mint] = float(ui_amount)

        return holdings

    async def close(self) -> None:
        await self._client.aclose()


def try_create_holdings_lookup() -> HeliusWalletHoldingsLookup | None:
    """Create a HeliusWalletHoldingsLookup if Helius RPC and wallet public key are available.

    Returns None if either is missing, so the readiness gate can report
    wallet_holdings_lookup_unavailable instead of wallet_holdings_unknown.
    """
    rpc_url = _resolve_helius_rpc_url()
    public_key = _resolve_trading_wallet_public_key()
    if not rpc_url or not public_key:
        return None
    return HeliusWalletHoldingsLookup(rpc_url=rpc_url, wallet_public_key=public_key)


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
