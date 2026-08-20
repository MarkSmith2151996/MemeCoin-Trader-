"""Coverage: JupiterPriceProvider (MT-602) for read-only paper mark prices.

All tests use fake HTTP clients — no real network calls.
"""

from __future__ import annotations

import asyncio

import httpx

from src.execution.price_provider import JupiterPriceProvider

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_MINT = "TokenMint1111111111111111111111111111111111"


def _price_payload(
    token_usd: float | None = 0.000021,
    sol_usd: float | None = 87.5,
    include_token: bool = True,
    include_sol: bool = True,
    extra_fields: dict | None = None,
) -> dict:
    """Build a Jupiter Price API v3-style response payload."""
    payload: dict = {}
    if include_token and token_usd is not None:
        entry = {
            "createdAt": "2026-08-20T00:00:00.000Z",
            "liquidity": 50000.0,
            "usdPrice": token_usd,
            "blockId": 440000000,
            "decimals": 6,
            "priceChange24h": 0.0,
        }
        if extra_fields:
            entry.update(extra_fields)
        payload[TOKEN_MINT] = entry
    if include_sol and sol_usd is not None:
        payload[WRAPPED_SOL_MINT] = {
            "createdAt": "2024-06-05T08:55:25.527Z",
            "liquidity": 708971414.6,
            "usdPrice": sol_usd,
            "blockId": 440466626,
            "decimals": 9,
            "priceChange24h": 1.0,
        }
    return payload


def _transport(payload: dict | None = None, status: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=payload or {})

    return httpx.MockTransport(handler)


def _error_transport(status: int = 500) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="error")

    return httpx.MockTransport(handler)


def _timeout_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    return httpx.MockTransport(handler)


# Test 1: provider returns SOL mark price from fake Jupiter price payload
def test_provider_returns_sol_price_from_jupiter() -> None:
    client = httpx.AsyncClient(transport=_transport(_price_payload(token_usd=0.000021, sol_usd=87.5)))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is not None
    assert abs(result.price_sol - (0.000021 / 87.5)) < 1e-12
    assert result.reason == "live_jupiter"
    assert result.liquidity_usd == 50000.0


# Test 2: provider returns unavailable when the mint is omitted (no reliable price)
def test_provider_unavailable_on_missing_mint() -> None:
    client = httpx.AsyncClient(transport=_transport(_price_payload(include_token=False)))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "no_price"


# Test 3: provider returns unavailable when SOL price is missing
def test_provider_unavailable_on_missing_sol_price() -> None:
    client = httpx.AsyncClient(transport=_transport(_price_payload(include_sol=False)))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "no_sol_price"


# Test 4: provider rejects non-positive / non-finite USD prices
def test_provider_rejects_invalid_token_price() -> None:
    import json

    for raw in (0, -1.0, float("nan"), float("inf")):
        transport = httpx.MockTransport(
            lambda request, raw=raw: httpx.Response(
                200,
                text=json.dumps(_price_payload(token_usd=raw), allow_nan=True),
            )
        )
        provider = JupiterPriceProvider(http_client=httpx.AsyncClient(transport=transport))

        result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

        assert result.price_sol is None
        assert result.reason == "invalid_price"


def test_provider_rejects_invalid_sol_price() -> None:
    client = httpx.AsyncClient(transport=_transport(_price_payload(sol_usd=0.0)))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "invalid_sol_price"


# Test 5: provider returns unavailable on malformed payload
def test_provider_unavailable_on_malformed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "malformed_response"


# Test 6: provider handles timeout / provider errors safely
def test_provider_handles_timeout() -> None:
    client = httpx.AsyncClient(transport=_timeout_transport())
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "provider_timeout"


def test_provider_handles_http_error() -> None:
    client = httpx.AsyncClient(transport=_error_transport(500))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "provider_error"


def test_provider_handles_rate_limit() -> None:
    client = httpx.AsyncClient(transport=_error_transport(429))
    provider = JupiterPriceProvider(http_client=client)

    result = asyncio.run(provider.get_price_with_diagnostic(TOKEN_MINT))

    assert result.price_sol is None
    assert result.reason == "rate_limited"


# Test 7: get_current_price mirrors the diagnostic price (None on failure)
def test_get_current_price_returns_sol_or_none() -> None:
    ok_client = httpx.AsyncClient(transport=_transport(_price_payload(token_usd=0.000021, sol_usd=87.5)))
    ok_provider = JupiterPriceProvider(http_client=ok_client)
    price = asyncio.run(ok_provider.get_current_price(TOKEN_MINT))
    assert price is not None
    assert abs(price - (0.000021 / 87.5)) < 1e-12

    fail_client = httpx.AsyncClient(transport=_transport({}))
    fail_provider = JupiterPriceProvider(http_client=fail_client)
    assert asyncio.run(fail_provider.get_current_price(TOKEN_MINT)) is None
