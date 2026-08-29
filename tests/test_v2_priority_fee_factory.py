"""V2 factory coverage for its shared live Jupiter priority-fee provider."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from services.adapters.live import build_live_adapter


class FakePriorityFeeProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False

    async def get_fee_lamports(self) -> int:
        self.calls += 1
        return 75_000

    async def close(self) -> None:
        self.closed = True


class FakeJupiterSwapClient:
    def __init__(self, *, priority_fee_callback) -> None:
        self.priority_fee_callback = priority_fee_callback
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeLiveExecutionAdapter:
    def __init__(self, *, client) -> None:
        self._client = client
        self._circuit_breaker = _Breaker()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeDirectExecutor:
    async def close(self) -> None:
        pass


class _Breaker:
    def status(self):
        return type("Status", (), {"tripped": False})()


def test_v2_factory_shares_one_p75_provider_for_entry_and_exit_callbacks() -> None:
    async def run() -> None:
        provider = FakePriorityFeeProvider()
        captured_client: FakeJupiterSwapClient | None = None

        def create_provider() -> FakePriorityFeeProvider:
            return provider

        def create_client(*, priority_fee_callback) -> FakeJupiterSwapClient:
            nonlocal captured_client
            captured_client = FakeJupiterSwapClient(
                priority_fee_callback=priority_fee_callback,
            )
            return captured_client

        with (
            patch("services.adapters.live.PriorityFeeProvider", side_effect=create_provider),
            patch("services.adapters.live.JupiterSwapClient", side_effect=create_client),
            patch("services.adapters.live.LiveExecutionAdapter", FakeLiveExecutionAdapter),
            patch("services.adapters.live.DirectExecutor", FakeDirectExecutor),
        ):
            router = build_live_adapter()

        assert captured_client is not None
        assert captured_client.priority_fee_callback.__self__ is provider
        entry_fee = await captured_client.priority_fee_callback()
        exit_fee = await captured_client.priority_fee_callback()

        assert entry_fee == exit_fee == 75_000
        assert provider.calls == 2
        assert router._priority_fee_provider is provider
        await router.close()
        assert provider.closed is True

    asyncio.run(run())
