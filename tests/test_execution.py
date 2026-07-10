import asyncio
import math

from src.core.models import Side
from src.execution.paper import PaperExecutionAdapter
from src.execution.price_provider import FakePriceProvider


def test_paper_adapter_executes_simulated_buy() -> None:
    async def run():
        adapter = PaperExecutionAdapter({"mint": 0.25})
        trade = await adapter.execute_swap("mint", Side.BUY, 1.0)
        await adapter.close()
        return trade

    trade = asyncio.run(run())

    assert trade.mode == "paper"
    assert trade.status == "simulated"
    assert trade.token_amount == 4.0


def test_paper_adapter_uses_price_provider_quote() -> None:
    async def run():
        adapter = PaperExecutionAdapter(price_provider=FakePriceProvider({"mint": 0.5}))
        trade = await adapter.execute_swap("mint", Side.BUY, 1.0)
        await adapter.close()
        return trade

    trade = asyncio.run(run())

    assert trade.price_sol == 0.5
    assert trade.token_amount == 2.0


def test_paper_adapter_falls_back_to_static_lookup_when_provider_misses() -> None:
    async def run():
        adapter = PaperExecutionAdapter(
            {"mint": 0.25},
            price_provider=FakePriceProvider({}),
        )
        trade = await adapter.execute_swap("mint", Side.BUY, 1.0)
        await adapter.close()
        return trade

    trade = asyncio.run(run())

    assert trade.price_sol == 0.25
    assert trade.token_amount == 4.0


def test_paper_adapter_rejects_nonfinite_prices() -> None:
    async def run() -> list[tuple[float | None, float | None]]:
        results = []
        for price in (math.nan, math.inf, -math.inf):
            adapter = PaperExecutionAdapter(price_provider=FakePriceProvider({"mint": price}))
            trade = await adapter.execute_swap("mint", Side.BUY, 1.0)
            results.append((trade.price_sol, trade.token_amount))
        return results

    assert asyncio.run(run()) == [(None, 0.0), (None, 0.0), (None, 0.0)]
