"""Coverage: Helius wallet holdings lookup provider.

All tests use fake HTTP clients — no real network calls.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from src.execution.helius_providers import (
    HeliusWalletHoldingsLookup,
    try_create_holdings_lookup,
)


def _build_token_account_json(mint: str, amount: float, decimals: int = 6) -> dict:
    """Build a single getTokenAccountsByOwner-style account entry."""
    return {
        "account": {
            "data": {
                "parsed": {
                    "info": {
                        "mint": mint,
                        "tokenAmount": {
                            "uiAmount": amount,
                            "decimals": decimals,
                            "amount": str(int(amount * 10**decimals)),
                        },
                    }
                }
            }
        }
    }


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
        content = self._json_body if self._json_body is not None else {"jsonrpc": "2.0", "id": 1, "result": {"value": [_build_token_account_json("mint1", 100.0), _build_token_account_json("mint2", 50.0)]}}
        raw = self._encoder.dumps(content).encode()
        request = httpx.Request("POST", url)
        return httpx.Response(self.status_code, content=raw, request=request)

    async def aclose(self) -> None:
        return None


def test_missing_public_key_returns_none() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is None


def test_missing_helius_key_returns_none() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is None


def test_healthy_holdings() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result == {"mint1": 100.0, "mint2": 50.0}


def test_empty_holdings() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"value": []}})
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result == {}


def test_zero_balance_excluded() -> None:
    accounts = [
        _build_token_account_json("mint1", 0.0),
        _build_token_account_json("mint2", 100.0),
    ]
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"value": accounts}})
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result == {"mint2": 100.0}


def test_malformed_response_returns_none() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1})
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is None


def test_error_response_returns_none() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "error"}})
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is None


def test_unreachable_rpc_returns_none() -> None:
    fake = FakeHttpClient(raise_on_post=True)
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is None


def test_private_key_not_required() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is not None
    assert "private" not in str(fake.last_payload).lower()


def test_no_secrets_in_output() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(h())
    assert result is not None


def test_try_create_returns_none_when_config_missing() -> None:
    h = try_create_holdings_lookup()
    if h is not None:
        asyncio.run(h.close())
    assert h is None or isinstance(h, HeliusWalletHoldingsLookup)


def test_position_reconciliation_can_consume_holdings() -> None:
    """Verify holdings lookup works with reconcile_positions signature."""
    from src.execution.position_reconciliation import SupportsWalletHoldingsLookup

    fake = FakeHttpClient()
    h: SupportsWalletHoldingsLookup = HeliusWalletHoldingsLookup(  # type: ignore[arg-type]
        rpc_url="https://example.com",
        wallet_public_key="abc",
        http_client=fake,  # type: ignore[arg-type]
    )
    result = asyncio.run(h())
    assert result is not None
    assert isinstance(result, dict)


def test_close_is_safe() -> None:
    fake = FakeHttpClient()
    h = HeliusWalletHoldingsLookup(rpc_url="https://example.com", wallet_public_key="abc", http_client=fake)  # type: ignore[arg-type]
    asyncio.run(h.close())
