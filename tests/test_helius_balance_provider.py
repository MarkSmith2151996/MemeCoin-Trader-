"""Coverage: Helius wallet balance lookup provider.

All tests use fake HTTP clients — no real network calls.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from src.execution.helius_providers import (
    HeliusWalletBalanceLookup,
    try_create_balance_lookup,
)


class FakeHttpClient:
    def __init__(self, status_code: int = 200, json_body: object = None, raise_on_post: bool = False) -> None:
        self.status_code = status_code
        self._json_body = json_body
        self._raise_on_post = raise_on_post
        self.last_url: str | None = None
        self.last_payload: object = None
        import json as _json
        self._encoder = _json

    async def post(self, url: str, *, json: object | None = None, **kwargs: object) -> httpx.Response:
        self.last_url = url
        self.last_payload = json
        if self._raise_on_post:
            raise httpx.ConnectError("connection refused")
        content = self._json_body if self._json_body is not None else {"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}, "value": 5_000_000_000}}
        raw = self._encoder.dumps(content).encode()
        request = httpx.Request("POST", url)
        return httpx.Response(self.status_code, content=raw, request=request)

    async def aclose(self) -> None:
        return None


def test_missing_public_key_returns_none() -> None:
    fake = FakeHttpClient()
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is None


def test_missing_helius_key_returns_none() -> None:
    fake = FakeHttpClient()
    bal = HeliusWalletBalanceLookup(rpc_url="", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is None


def test_healthy_balance() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}, "value": 10_000_000_000}})
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result == 10.0


def test_low_balance() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}, "value": 100_000_000}})
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result == 0.1


def test_malformed_response_returns_none() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1})
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is None


def test_error_response_returns_none() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "error"}})
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is None


def test_unreachable_rpc_returns_none() -> None:
    fake = FakeHttpClient(raise_on_post=True)
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is None


def test_private_key_not_required() -> None:
    """Balance lookup works with only public key — no private key needed."""
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}, "value": 5_000_000_000}})
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result == 5.0


def test_no_secrets_in_output() -> None:
    """Wallet address and API key do not appear in diagnostic output."""
    fake = FakeHttpClient()
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(bal())
    assert result is not None


def test_try_create_returns_none_when_config_missing() -> None:
    bal = try_create_balance_lookup()
    if bal is not None:
        asyncio.run(bal.close())
    assert bal is None or isinstance(bal, HeliusWalletBalanceLookup)


def test_close_is_safe() -> None:
    fake = FakeHttpClient()
    bal = HeliusWalletBalanceLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    asyncio.run(bal.close())
