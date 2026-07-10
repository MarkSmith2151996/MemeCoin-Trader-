"""Coverage: DexScreenerPriceProvider for read-only paper mark prices.

All tests use fake HTTP clients — no real network calls.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import Trade
from src.execution.paper_pnl import PaperPnLCalculator
from src.execution.price_provider import (
    DexScreenerPriceProvider,
    FakePriceProvider,
    UnavailablePriceProvider,
)
from src.strategy.position_manager import PositionManager


runner = CliRunner()


def _fake_transport(pairs: list[dict] | None = None, status: int = 200) -> httpx.MockTransport:
    """Build a mock transport that returns a DexScreener-style response."""
    payload: dict = {"schemaVersion": "1.0.0", "pairs": pairs or []}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=payload)

    return httpx.MockTransport(handler)


def _error_transport(status: int = 500) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="error")

    return httpx.MockTransport(handler)


def _timeout_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    return httpx.MockTransport(handler)


def _paper_position(manager: PositionManager, mint: str, amount_sol: float = 1.0, price_sol: float = 0.00001) -> None:
    trade = Trade(
        mint_address=mint,
        side="BUY",
        amount_sol=amount_sol,
        token_amount=amount_sol / price_sol,
        price_sol=price_sol,
        mode="paper",
        status="simulated",
    )
    asyncio.run(manager.open_position(trade, None))


SOL_PAIR = {
    "chainId": "solana",
    "dexId": "raydium",
    "pairAddress": "PairAddr111111111111111111111111111111111111",
    "priceNative": "0.000015",
    "priceUsd": "0.0021",
    "liquidity": {"usd": 50000},
    "baseToken": {"address": "TokenMint1111111111111111111111111111111111", "symbol": "TEST", "name": "Test Token"},
    "quoteToken": {"address": "So11111111111111111111111111111111111111112", "symbol": "SOL", "name": "Wrapped SOL"},
}


# Test 1: provider returns SOL mark price from fake DexScreener-style payload
def test_provider_returns_sol_price_from_dexscreener() -> None:
    transport = _fake_transport(pairs=[SOL_PAIR])
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is not None
        assert result.price_sol == 0.000015
        assert result.reason == "live_dexscreener"

    asyncio.run(run())


# Test 2: provider returns unavailable on no pairs
def test_provider_unavailable_on_no_pairs() -> None:
    transport = _fake_transport(pairs=None)
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is None
        assert result.reason == "no_pairs"

    asyncio.run(run())


# Test 3: provider returns unavailable on malformed payload
def test_provider_unavailable_on_malformed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is None
        assert result.reason == "malformed_response"

    asyncio.run(run())


# Test 4: provider returns unavailable when only non-solana pair exists
def test_provider_unavailable_on_no_solana_pairs() -> None:
    eth_pair = {
        "chainId": "ethereum",
        "dexId": "uniswap",
        "pairAddress": "0xPairAddr1111111111111111111111111111111",
        "priceNative": "0.000015",
        "priceUsd": "0.0021",
        "liquidity": {"usd": 50000},
    }
    transport = _fake_transport(pairs=[eth_pair])
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is None
        assert result.reason == "no_solana_pairs"

    asyncio.run(run())


# Test 5: provider handles timeout/provider errors safely
def test_provider_handles_timeout() -> None:
    transport = _timeout_transport()
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is None
        assert result.reason == "provider_timeout"

    asyncio.run(run())


def test_provider_handles_http_error() -> None:
    transport = _error_transport(500)
    client = httpx.AsyncClient(transport=transport)
    provider = DexScreenerPriceProvider(http_client=client)

    async def run() -> None:
        result = await provider.get_price_with_diagnostic("TokenMint1111111111111111111111111111111111")
        assert result.price_sol is None
        assert result.reason == "provider_error"

    asyncio.run(run())


# Test 6: paper-pnl with fake live marks calculates unrealized PnL
def test_paper_pnl_with_fake_marks_shows_unrealized(tmp_path: Path) -> None:
    db = tmp_path / "live_marks.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "FakeMarkMint11111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    fake_prices = FakePriceProvider({"FakeMarkMint11111111111111111111111111111111": 0.00002})
    calculator = PaperPnLCalculator(manager, price_provider=fake_prices)
    summary = asyncio.run(calculator.compute_summary())

    assert summary.marks_mode == "live"
    assert summary.unrealized_pnl_sol is not None
    assert summary.unrealized_pnl_sol == 1.0  # 100000 * 0.00002 - 1.0
    assert summary.mark_unavailable_count == 0
    assert summary.unrealized_incomplete is False


# Test 7: paper-pnl does not invent PnL when marks unavailable
def test_paper_pnl_no_invented_pnl(tmp_path: Path) -> None:
    db = tmp_path / "no_invent.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "NoPriceMint1111111111111111111111111111111111")

    calculator = PaperPnLCalculator(manager, price_provider=UnavailablePriceProvider())
    summary = asyncio.run(calculator.compute_summary())

    assert summary.marks_mode == "unavailable"
    assert summary.unrealized_pnl_sol is None
    assert summary.mark_unavailable_count == 1


# Test 8: fake/mock mints with no provider price remain mark-unavailable
def test_paper_pnl_fake_mint_no_price_remains_unavailable(tmp_path: Path) -> None:
    db = tmp_path / "fake_mint_no_price.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "FakeUnlisted11111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    fake_prices = FakePriceProvider({})  # no prices
    calculator = PaperPnLCalculator(manager, price_provider=fake_prices)
    summary = asyncio.run(calculator.compute_summary())

    assert summary.marks_mode == "live"
    assert summary.unrealized_pnl_sol is None
    assert summary.mark_unavailable_count == 1
    assert summary.unrealized_incomplete is True
    assert summary.positions[0].mark_reason == "price_unavailable"


# Test 9: paper-close uses mark only when explicitly requested
def test_paper_close_use_mark_flag(tmp_path: Path) -> None:
    db = tmp_path / "use_mark_close.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    manager = PositionManager(db, settings)
    _paper_position(manager, "UseMarkMint111111111111111111111111111111111", amount_sol=1.0, price_sol=0.00001)

    # Without --use-mark, should fail since no --price
    result = runner.invoke(
        cli_module.app,
        ["paper-close", "--mint", "UseMarkMint111111111111111111111111111111111", "--db-path", str(db)],
    )
    assert result.exit_code != 0
    assert "No exit price" in result.stdout

    # Close via API with FakePriceProvider to simulate --use-mark behavior
    fake_provider = FakePriceProvider({"UseMarkMint111111111111111111111111111111111": 0.00002})
    mark_price = asyncio.run(fake_provider.get_current_price("UseMarkMint111111111111111111111111111111111"))
    assert mark_price == 0.00002

    closed = asyncio.run(manager.close_position("UseMarkMint111111111111111111111111111111111", exit_price_sol=mark_price))
    assert closed is not None
    assert closed.status.value == "CLOSED"
    assert closed.realized_pnl_sol == 1.0  # 100000 * 0.00002 - 1.0


# Test 10: no private key or wallet env is required
def test_no_private_key_required(tmp_path: Path) -> None:
    import os
    assert "TRADING_WALLET_PRIVATE_KEY" not in os.environ or os.environ["TRADING_WALLET_PRIVATE_KEY"] == ""

    db = tmp_path / "no_key.db"
    result = runner.invoke(cli_module.app, ["paper-pnl", "--marks", "live", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "Paper PnL Summary" in result.stdout


# Test 11: no secrets are printed
def test_paper_pnl_no_secrets_printed(tmp_path: Path) -> None:
    db = tmp_path / "secrets.db"
    result = runner.invoke(cli_module.app, ["paper-pnl", "--marks", "live", "--db-path", str(db)])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output
    assert "api-key=" not in output
    assert "rpc_url=" not in output


# Test 12: existing paper-soak flow still passes
def test_paper_soak_still_passes(tmp_path: Path) -> None:
    from src.core.models import Signal, SignalType, SignalSource as SignalSourceEnum
    from src.signals.base import SignalSource

    class FakeSoakSource(SignalSource):
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        @property
        def name(self) -> str:
            return "test_soak"

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def poll(self) -> list[Signal]:
            return []

    db = tmp_path / "soak.db"
    asyncio.run(init_db(db))
    settings = load_settings()
    source = FakeSoakSource()

    summary = asyncio.run(
        cli_module.run_bounded_paper_cycle(
            max_signals=10,
            timeout_seconds=0.1,
            db_path=db,
            sources=[source],
            poll_interval_s=0.0,
        )
    )

    assert summary.signals_collected == 0
    assert summary.termination_reason == "timeout"
    assert source.started is True
    assert source.stopped is True


def test_paper_pnl_shows_marks_live_flag(tmp_path: Path) -> None:
    db = tmp_path / "flag_test.db"
    result = runner.invoke(cli_module.app, ["paper-pnl", "--marks", "live", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "live" in result.stdout.lower()


def test_paper_pnl_shows_marks_unavailable_flag(tmp_path: Path) -> None:
    db = tmp_path / "flag_unavail.db"
    result = runner.invoke(cli_module.app, ["paper-pnl", "--marks", "unavailable", "--db-path", str(db)])
    assert result.exit_code == 0
    assert "unavailable" in result.stdout.lower()
