"""Coverage: resolve_mint from scripts/run_paper_loop.py.

All tests use httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from scripts.run_paper_loop import fetch_entry_metadata, resolve_mint


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


def test_fetch_entry_metadata_success() -> None:
    """Full pair payload → entry_* metadata dict with computed age hours."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {
                    "chainId": "solana",
                    "baseToken": {"address": "Abcd1234"},
                    "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
                    "marketCap": 123456.0,
                    "volume": {"h24": 50000.0, "h1": 1200.0},
                    "txns": {"h24": {"buys": 40, "sells": 10}, "h1": {"buys": 5, "sells": 1}},
                    "liquidity": {"usd": 25000.0},
                    "pairCreatedAt": 1754179200000,
                    "priceChange": {"h1": 0.25, "m5": -0.03},
                    "fdv": 200000.0,
                }
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(fetch_entry_metadata("Abcd1234", client))
    assert result["quote_provider"] == "paper"
    assert result["entry_mcap"] == 123456.0
    assert result["entry_volume_24h"] == 50000.0
    assert result["entry_volume_1h"] == 1200.0
    assert result["entry_txns_24h"] == 50
    assert result["entry_txns_1h"] == 6
    assert result["entry_liquidity_usd"] == 25000.0
    assert result["entry_price_change_1h"] == 0.25
    assert result["entry_price_change_5m"] == -0.03
    assert result["entry_fdv"] == 200000.0
    assert result["dexscreener"] == {
        "mcap": 123456.0,
        "volume": {"h24": 50000.0, "h1": 1200.0},
        "txns": {"h24": {"buys": 40, "sells": 10}, "h1": {"buys": 5, "sells": 1}},
        "liquidity": {"usd": 25000.0},
        "fdv": 200000.0,
        "age_hours": result["entry_age_hours"],
        "price_usd": None,
        "price_change": {"h1": 0.25, "m5": -0.03},
    }
    assert isinstance(result["entry_age_hours"], float)
    assert result["entry_age_hours"] >= 0


def test_fetch_entry_metadata_no_match_returns_empty() -> None:
    """No Solana pair with the requested base mint → empty dict (no blocking)."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {"chainId": "ethereum", "baseToken": {"address": "0xother"}},
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(fetch_entry_metadata("Abcd1234", client))
    assert result == {}


def test_fetch_entry_metadata_api_error_returns_empty() -> None:
    """HTTP 500 → empty dict (no exception propagates, entry not blocked)."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal error")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(fetch_entry_metadata("Abcd1234", client))
    assert result == {}


def test_fetch_entry_metadata_missing_fields_store_none() -> None:
    """Sparse pair payload → missing fields stored as None, not raised."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "pairs": [
                {"chainId": "solana", "baseToken": {"address": "Abcd1234"}},
            ]
        })

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    result = asyncio.run(fetch_entry_metadata("Abcd1234", client))
    assert result["entry_mcap"] is None
    assert result["entry_age_hours"] is None
    assert result["entry_txns_24h"] == 0
