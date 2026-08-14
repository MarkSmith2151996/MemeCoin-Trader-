"""Coverage: live execution adapter (buy/sell, pre-swap gates, fill recording).

Uses a fake JupiterSwapClient — no real network or wallet keys.
"""

from __future__ import annotations

import asyncio

import pytest

from src.chain.jupiter import SOL_MINT
from src.chain.jupiter_swap import JupiterSwapQuote, JupiterSwapResult
from src.core.models import Side
from src.execution.live import LiveExecutionAdapter

TOKEN_MINT = "tok12345678901234567890123456789012"


class FakeSwapClient:
    def __init__(
        self,
        *,
        decimals: int = 6,
        sol_balance: float = 5.0,
        token_balance: float = 1000.0,
        quote_impact_pct: float = 0.01,
        swap_ok: bool = True,
        quote_none: bool = False,
    ) -> None:
        self.decimals = decimals
        self.sol_balance = sol_balance
        self.token_balance = token_balance
        self.quote_impact_pct = quote_impact_pct
        self.swap_ok = swap_ok
        self.quote_none = quote_none
        self.swap_calls: list[JupiterSwapQuote] = []
        self.closed = False

    async def get_token_decimals(self, mint: str) -> int:
        return self.decimals

    async def get_quote(self, input_mint, output_mint, amount_lamports, slippage_bps=100):
        if self.quote_none:
            return None
        return JupiterSwapQuote(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=amount_lamports,
            out_amount=1_000_000,
            price_impact_pct=self.quote_impact_pct,
            slippage_bps=slippage_bps,
            token_decimals=self.decimals,
            price_sol=0.05,
            raw={"inAmount": str(amount_lamports), "outAmount": "1000000"},
        )

    async def execute_swap(self, quote: JupiterSwapQuote) -> JupiterSwapResult:
        self.swap_calls.append(quote)
        if not self.swap_ok:
            return JupiterSwapResult(
                ok=False,
                signature=None,
                input_mint=quote.input_mint,
                output_mint=quote.output_mint,
                in_amount=quote.in_amount,
                out_amount=quote.out_amount,
                price_sol=quote.price_sol,
                fees_lamports=5000,
                confirmation_status="failed",
                slot=None,
                attempts=1,
                error="swap rejected by test",
            )
        return JupiterSwapResult(
            ok=True,
            signature="sig-abc",
            input_mint=quote.input_mint,
            output_mint=quote.output_mint,
            in_amount=quote.in_amount,
            out_amount=quote.out_amount,
            price_sol=quote.price_sol,
            fees_lamports=5000,
            confirmation_status="confirmed",
            slot=77,
            attempts=1,
            token_balance_after=self.token_balance,
        )

    async def get_sol_balance(self) -> float | None:
        return self.sol_balance

    async def get_token_balance(self, mint: str) -> float | None:
        return self.token_balance

    async def close(self) -> None:
        self.closed = True


def _adapter(**overrides) -> LiveExecutionAdapter:
    return LiveExecutionAdapter(client=FakeSwapClient(**overrides))


def test_buy_happy_path_records_fill() -> None:
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client)

    trade = asyncio.run(adapter.buy(TOKEN_MINT, 0.05))

    assert trade.mint_address == TOKEN_MINT
    assert trade.side == Side.BUY
    assert trade.amount_sol == 0.05
    assert trade.token_amount == pytest.approx(1.0)
    assert trade.price_sol == pytest.approx(0.05)
    assert trade.tx_signature == "sig-abc"
    assert trade.mode == "live"
    assert trade.status == "confirmed"
    assert trade.metadata["price_impact_pct"] == pytest.approx(0.01)
    assert trade.metadata["fees_lamports"] == 5000
    assert len(client.swap_calls) == 1
    assert client.swap_calls[0].input_mint == SOL_MINT


def test_buy_rejects_banned_token() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(), banned_tokens={TOKEN_MINT})

    with pytest.raises(RuntimeError, match="banned"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_rejects_insufficient_sol() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(sol_balance=0.001))

    with pytest.raises(RuntimeError, match="insufficient SOL"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_blocks_when_balance_unverifiable() -> None:
    client = FakeSwapClient()
    client.sol_balance = None
    adapter = LiveExecutionAdapter(client=client)

    with pytest.raises(RuntimeError, match="cannot verify"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_rejects_high_price_impact() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(quote_impact_pct=0.06))

    with pytest.raises(RuntimeError, match="price impact"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_rejects_zero_output_quote() -> None:
    client = FakeSwapClient()

    async def zero_quote(input_mint, output_mint, amount_lamports, slippage_bps=100):
        return JupiterSwapQuote(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=amount_lamports,
            out_amount=0,
            price_impact_pct=0.001,
            slippage_bps=slippage_bps,
            token_decimals=6,
            price_sol=None,
            raw={},
        )

    client.get_quote = zero_quote
    adapter = LiveExecutionAdapter(client=client)

    with pytest.raises(RuntimeError, match="zero output"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_raises_when_swap_fails() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(swap_ok=False))

    with pytest.raises(RuntimeError, match="live buy failed"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_buy_raises_when_quote_none() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(quote_none=True))

    with pytest.raises(RuntimeError, match="quote failed"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))


def test_sell_happy_path_records_fill() -> None:
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client)

    trade = asyncio.run(adapter.sell(TOKEN_MINT, 100.0))

    assert trade.side == Side.SELL
    assert trade.token_amount == 100.0
    assert trade.tx_signature == "sig-abc"
    assert trade.mode == "live"
    assert trade.status == "confirmed"
    assert len(client.swap_calls) == 1
    assert client.swap_calls[0].output_mint == SOL_MINT
    # 1M token lamports in -> 1M lamports out -> 0.001 SOL
    assert trade.amount_sol == pytest.approx(0.001)


def test_sell_rejects_non_positive_token_amount() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient())

    with pytest.raises(RuntimeError, match="non-positive"):
        asyncio.run(adapter.sell(TOKEN_MINT, 0.0))


def test_sell_rejects_banned_token() -> None:
    adapter = LiveExecutionAdapter(client=FakeSwapClient(), banned_tokens={TOKEN_MINT})

    with pytest.raises(RuntimeError, match="banned"):
        asyncio.run(adapter.sell(TOKEN_MINT, 100.0))


def test_execute_swap_dispatches_buy_and_sell() -> None:
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client)

    buy_trade = asyncio.run(adapter.execute_swap(TOKEN_MINT, Side.BUY, 0.05))
    assert buy_trade.side == Side.BUY

    sell_trade = asyncio.run(adapter.execute_swap(TOKEN_MINT, Side.SELL, 0.05))
    assert sell_trade.side == Side.SELL


def test_execute_swap_sell_without_balance_raises() -> None:
    client = FakeSwapClient()
    client.token_balance = None
    adapter = LiveExecutionAdapter(client=client)

    with pytest.raises(RuntimeError, match="wallet holds no"):
        asyncio.run(adapter.execute_swap(TOKEN_MINT, Side.SELL, 0.05))


def test_get_quote_buy() -> None:
    adapter = _adapter()

    quote = asyncio.run(adapter.get_quote(TOKEN_MINT, Side.BUY, 0.05))

    assert quote.provider == "live"
    assert quote.price_sol == pytest.approx(0.05)
    assert quote.estimated_out_amount == pytest.approx(1.0)
    assert quote.price_impact_pct == pytest.approx(0.01)


def test_get_current_price_uses_reference_provider_first() -> None:
    class FakePriceProvider:
        async def get_current_price(self, mint):
            return 0.123

    adapter = LiveExecutionAdapter(
        client=FakeSwapClient(), reference_price_provider=FakePriceProvider(),
    )
    price = asyncio.run(adapter.get_current_price(TOKEN_MINT))
    assert price == pytest.approx(0.123)


def test_get_current_price_falls_back_to_quote() -> None:
    adapter = _adapter()
    price = asyncio.run(adapter.get_current_price(TOKEN_MINT))
    assert price == pytest.approx(0.05)


def test_close_marks_adapter_closed() -> None:
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client)

    asyncio.run(adapter.close())

    assert client.closed is True
    with pytest.raises(RuntimeError, match="closed"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))
