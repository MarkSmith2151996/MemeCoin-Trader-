"""Live V2 adapter factory: direct Pump.fun first, Jupiter fallback."""

from __future__ import annotations

from src.execution.direct import DirectExecutor
from src.execution.live import LiveExecutionAdapter
from src.execution.pumpfun_router import PumpFunExecutionRouter


class V2PumpFunExecutionRouter(PumpFunExecutionRouter):
    """Expose the existing live breaker to the V2 fail-closed supervisor."""

    def circuit_breaker_tripped(self) -> bool:
        return self._jupiter._circuit_breaker.status().tripped


def build_live_adapter() -> V2PumpFunExecutionRouter:
    """Build the existing guarded direct/Jupiter routing stack."""

    jupiter = LiveExecutionAdapter()
    return V2PumpFunExecutionRouter(jupiter, DirectExecutor())
