"""Coverage: resolve_mint from scripts/run_paper_loop.py.

All tests use httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from scripts.run_paper_loop import resolve_mint


def test_successful_resolution() -> None:
    """DexScreener returns a Solana pair with WSOL quote → mint address returned."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {
                    "chainId": "solana",
                    "baseToken": {"address": "Abcd1234"},
                    "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
                }
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(resolve_mint("testcoin", client))
    assert result == "Abcd1234"


def test_non_solana_pair_filtered() -> None:
    """Pair has chainId='ethereum' → returns None."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {
                    "chainId": "ethereum",
                    "baseToken": {"address": "0x1234"},
                    "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
                }
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(resolve_mint("testcoin", client))
    assert result is None


def test_non_wsol_quote_filtered() -> None:
    """Pair has USDC as quote token → returns None."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {
                    "chainId": "solana",
                    "baseToken": {"address": "Abcd1234"},
                    "quoteToken": {"address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"},
                }
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(resolve_mint("testcoin", client))
    assert result is None


def test_empty_pairs() -> None:
    """DexScreener returns no pairs → returns None."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"pairs": []})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(resolve_mint("testcoin", client))
    assert result is None


def test_http_error() -> None:
    """DexScreener returns HTTP 500 → returns None (no exception propagates)."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal error")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(resolve_mint("testcoin", client))
    assert result is None
