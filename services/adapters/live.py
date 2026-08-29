"""Live V2 adapter factory: direct Pump.fun first, Jupiter fallback."""

from __future__ import annotations

from src.chain.jupiter_swap import JupiterSwapClient
from src.chain.priority_fee import PriorityFeeProvider
from src.execution.direct import DirectExecutor
from src.execution.live import LiveExecutionAdapter
from src.execution.pumpfun_router import PumpFunExecutionRouter


class V2PumpFunExecutionRouter(PumpFunExecutionRouter):
    """Expose V2 live controls and own its shared Jupiter fee provider."""

    def __init__(
        self,
        jupiter: LiveExecutionAdapter,
        direct: DirectExecutor,
        priority_fee_provider: PriorityFeeProvider,
    ) -> None:
        super().__init__(jupiter, direct)
        self._priority_fee_provider = priority_fee_provider

    def circuit_breaker_tripped(self) -> bool:
        return self._jupiter._circuit_breaker.status().tripped

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            await self._priority_fee_provider.close()


def build_live_adapter() -> V2PumpFunExecutionRouter:
    """Build V2's guarded routing stack with a shared cached p75 fee provider.

    The provider samples up to 20 recent RPC fees, refreshes at most every 30
    seconds, and clamps p75 to 1,000-1,000,000 lamports. The one instance is
    passed to Jupiter's client, so both fallback buy and sell swaps use the
    same cache. Empty or unavailable RPC samples retain Jupiter's safe legacy
    fee body rather than blocking an active live leg.
    """

    priority_fee_provider = PriorityFeeProvider()
    jupiter_client = JupiterSwapClient(
        priority_fee_callback=priority_fee_provider.get_fee_lamports,
    )
    jupiter = LiveExecutionAdapter(client=jupiter_client)
    return V2PumpFunExecutionRouter(jupiter, DirectExecutor(), priority_fee_provider)
