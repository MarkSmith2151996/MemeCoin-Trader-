import asyncio

from src.core.models import Side
from src.execution.paper import PaperExecutionAdapter


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
