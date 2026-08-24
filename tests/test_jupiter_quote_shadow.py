"""Coverage: shadow-mode Jupiter V2 quote client + persistence.

All tests use httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from scripts.run_strategy_b import _shadow_quote_and_record
from src.chain.jupiter_quote import JupiterQuoteV2, JupiterV2QuoteClient
from src.core.database import init_db, record_jupiter_quote
from src.core.models import Side

RPC_BODY = {"jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 6}}}


def _quote_response(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "inputMint": "So11111111111111111111111111111111111111112",
        "outputMint": "tokenmint",
        "inAmount": "50000000",
        "outAmount": "1000000",
        "priceImpactPct": "0.001",
        "routePlan": [{"swapInfo": {"marketInfos": []}, "percent": 100}],
    }
    payload.update(overrides)
    return payload


def _make_client(handler) -> JupiterV2QuoteClient:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return JupiterV2QuoteClient(http_client=client, min_interval_s=0.0)


def test_buy_quote_happy_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000))

    assert isinstance(quote, JupiterQuoteV2)
    assert quote.input_mint == "So11111111111111111111111111111111111111112"
    assert quote.output_mint == "tokenmint"
    assert quote.in_amount == 50_000_000
    assert quote.out_amount == 1_000_000
    assert quote.price_impact_pct == pytest.approx(0.001)
    assert len(quote.route_plan) == 1
    assert quote.token_decimals == 6
    assert quote.price_sol == pytest.approx(0.05)
    assert datetime.fromisoformat(quote.quoted_at).tzinfo is not None


def test_default_quote_endpoint_uses_public_resolving_host() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        assert request.url.host == "public.jupiterapi.com"
        assert request.url.path == "/quote"
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000))

    assert quote is not None


def test_sell_quote_happy_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(200, json=_quote_response(
            inputMint="tokenmint",
            outputMint="So11111111111111111111111111111111111111112",
            inAmount="1000000",
            outAmount="25000000",
        ))

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote("tokenmint", Side.SELL, 1_000_000))

    assert isinstance(quote, JupiterQuoteV2)
    assert quote.input_mint == "tokenmint"
    assert quote.output_mint == "So11111111111111111111111111111111111111112"
    assert quote.price_sol == pytest.approx(0.025)


def test_decimals_lookup_cached() -> None:
    rpc_call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal rpc_call_count
        if request.method == "POST":
            rpc_call_count += 1
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000))
    asyncio.run(client.get_quote("tokenmint", Side.BUY, 25_000_000))

    assert rpc_call_count == 1


def test_http_error_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(500, text="jupiter down")

    client = _make_client(handler)

    assert asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000)) is None


def test_rate_limit_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(429, text="rate limited")

    client = _make_client(handler)

    assert asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000)) is None


def test_no_route_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(404, json={"error": "No routes found"})

    client = _make_client(handler)

    assert asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000)) is None


def test_malformed_quote_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(200, json={"inAmount": "1000"})

    client = _make_client(handler)

    assert asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000)) is None


def test_decimals_rpc_failure_falls_back_and_quote_succeeds() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(500, text="rpc down")
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote("tokenmint", Side.BUY, 50_000_000))

    assert quote is not None
    assert quote.token_decimals == 9
    # outAmount 1_000_000 lamports @ 9 decimals = 0.001 tokens → 0.05 SOL / 0.001 = 50 SOL/token
    assert quote.price_sol == pytest.approx(50.0)


def test_non_positive_amount_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    assert asyncio.run(client.get_quote("tokenmint", Side.BUY, 0)) is None


def test_throttle_respects_min_interval() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_BODY)
        return httpx.Response(200, json=_quote_response())

    transport = httpx.MockTransport(handler)
    client = JupiterV2QuoteClient(
        http_client=httpx.AsyncClient(transport=transport),
        min_interval_s=0.05,
    )

    async def run() -> float:
        start = asyncio.get_event_loop().time()
        await client.get_quote("tokenmint", Side.BUY, 50_000_000)
        await client.get_quote("tokenmint", Side.BUY, 50_000_000)
        return asyncio.get_event_loop().time() - start

    elapsed = asyncio.run(run())
    assert elapsed >= 0.05


def test_record_jupiter_quote_round_trip(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    quoted_at = datetime.now(UTC).isoformat()

    asyncio.run(record_jupiter_quote(
        db_path,
        position_id="pos-1",
        side="buy",
        mint_address="tokenmint",
        dex_price_sol=0.000123,
        jup_output_amount=1_000_000,
        jup_price_sol=0.000119,
        price_impact_pct=0.02,
        slippage_vs_paper_pct=-3.25,
        route_info='[{"percent": 100}]',
        quoted_at=quoted_at,
    ))

    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT position_id, side, mint_address, dex_price_sol, jup_output_amount,"
            " jup_price_sol, price_impact_pct, slippage_vs_paper_pct, route_info, quoted_at"
            " FROM jupiter_quotes"
        ).fetchone()

    assert row == (
        "pos-1", "buy", "tokenmint", 0.000123, 1_000_000,
        0.000119, 0.02, -3.25, '[{"percent": 100}]', quoted_at,
    )


def test_init_db_creates_jupiter_quotes_table(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    with sqlite3.connect(db_path) as db:
        names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "jupiter_quotes" in names


class _FakeQuoteClient:
    def __init__(self, quote: JupiterQuoteV2 | None) -> None:
        self._quote = quote

    async def get_quote(self, mint: str, side: Side, amount_lamports: int) -> JupiterQuoteV2 | None:
        return self._quote


def _fake_quote() -> JupiterQuoteV2:
    return JupiterQuoteV2(
        input_mint="So11111111111111111111111111111111111111112",
        output_mint="tokenmint",
        in_amount=50_000_000,
        out_amount=1_000_000,
        price_impact_pct=0.001,
        route_plan=({"swapInfo": {}},),
        quoted_at=datetime.now(UTC).isoformat(),
        token_decimals=6,
        price_sol=0.05,
    )


def test_shadow_hook_records_quote_and_compares(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))

    quote = asyncio.run(_shadow_quote_and_record(
        mint="tokenmint",
        side=Side.BUY,
        amount_lamports=50_000_000,
        dex_price_sol=0.05,
        position_id="pos-1",
        db_path=db_path,
        client=_FakeQuoteClient(_fake_quote()),
    ))

    assert quote is not None
    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT position_id, side, mint_address, dex_price_sol,"
            " jup_output_amount, jup_price_sol, slippage_vs_paper_pct"
            " FROM jupiter_quotes"
        ).fetchone()
    assert row == ("pos-1", "buy", "tokenmint", 0.05, 1_000_000, 0.05, 0.0)


def test_shadow_hook_skips_when_quote_unavailable(tmp_path: Path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))

    quote = asyncio.run(_shadow_quote_and_record(
        mint="tokenmint",
        side=Side.BUY,
        amount_lamports=50_000_000,
        dex_price_sol=0.05,
        position_id="pos-1",
        db_path=db_path,
        client=_FakeQuoteClient(None),
    ))

    assert quote is None
    with sqlite3.connect(db_path) as db:
        count = db.execute("SELECT COUNT(*) FROM jupiter_quotes").fetchone()[0]
    assert count == 0
