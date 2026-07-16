"""Coverage: JupiterClient.get_quote() with decimals caching.

All tests use httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest

from src.chain.jupiter import JupiterClient
from src.core.models import Side, SwapQuote


def test_buy_quote() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}},
            })
        return httpx.Response(200, json={
            "inAmount": "10000000",
            "outAmount": "1000000",
            "priceImpactPct": "0.001",
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    quote = asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))

    assert isinstance(quote, SwapQuote)
    assert quote.mint_address == "mint123"
    assert quote.side == Side.BUY
    assert quote.amount_sol == 0.01
    assert quote.price_sol is not None
    assert abs(quote.price_sol - 0.01) < 1e-9
    assert abs(quote.estimated_out_amount - 1.0) < 1e-9
    assert quote.provider == "jupiter"
    assert quote.expires_at is not None
    assert quote.expires_at > datetime.now(UTC)


def test_decimals_cached() -> None:
    rpc_call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal rpc_call_count
        if request.method == "POST":
            rpc_call_count += 1
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}},
            })
        return httpx.Response(200, json={
            "inAmount": "10000000",
            "outAmount": "1000000",
            "priceImpactPct": "0.001",
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))
    asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.02))

    assert rpc_call_count == 1


def test_http_error_on_quote_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}},
            })
        return httpx.Response(429, text="rate limited")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))


def test_http_error_on_rpc_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal error")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))


def test_malformed_quote_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}},
            })
        return httpx.Response(200, json={"inAmount": "1000"})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    with pytest.raises(KeyError):
        asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))


def test_no_real_network_calls() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        if request.method == "POST":
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}},
            })
        return httpx.Response(200, json={
            "inAmount": "10000000",
            "outAmount": "1000000",
            "priceImpactPct": "0.001",
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    jupiter = JupiterClient(http_client=client)

    asyncio.run(jupiter.get_quote("mint123", Side.BUY, 0.01))
    assert called
