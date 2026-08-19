"""Focused offline coverage for the MT-588 dynamic priority fee provider."""

from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx

from src.chain.priority_fee import PriorityFeeProvider


def _rpc_response(fees: list[int]) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {"slot": 440160500 + i, "prioritizationFee": fee}
                for i, fee in enumerate(fees)
            ],
        },
    )


def _provider(
    fees: list[int],
    *,
    min_lamports: int = 0,
    max_lamports: int = 1_000_000,
    refresh_interval_s: float = 30.0,
) -> tuple[PriorityFeeProvider, list[dict[str, object]]]:
    calls: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content.decode()))
        return _rpc_response(fees)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = PriorityFeeProvider(
        rpc_url="http://rpc.test",
        client=client,
        min_lamports=min_lamports,
        max_lamports=max_lamports,
        refresh_interval_s=refresh_interval_s,
    )
    return provider, calls


def test_75th_percentile_of_recent_fees() -> None:
    async def run() -> int | None:
        provider, _ = _provider([1, 2, 3, 4, 5])
        try:
            return await provider.get_fee_lamports()
        finally:
            await provider.close()

    assert asyncio.run(run()) == 4


def test_fee_is_cached_within_refresh_interval() -> None:
    async def run() -> tuple[int | None, int]:
        provider, calls = _provider([10, 20, 30, 40, 50])
        try:
            await provider.get_fee_lamports()
            await provider.get_fee_lamports()
            await provider.get_fee_lamports()
        finally:
            await provider.close()
        return (len(calls),)

    (call_count,) = asyncio.run(run())
    assert call_count == 1


def test_refresh_after_interval() -> None:
    async def run() -> tuple[int | None, int | None, int]:
        provider, calls = _provider([10, 20, 30, 40, 50])
        try:
            first = await provider.get_fee_lamports()
            provider._fee_fetched_at = 0.0  # simulate a stale cache
            second = await provider.get_fee_lamports()
        finally:
            await provider.close()
        return (first, second, len(calls))

    first, second, call_count = asyncio.run(run())
    assert first == second == 40
    assert call_count == 2


def test_failed_lookup_returns_none_on_first_call() -> None:
    async def run() -> int | None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("rpc down")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = PriorityFeeProvider(rpc_url="http://rpc.test", client=client)
        try:
            return await provider.get_fee_lamports()
        finally:
            await provider.close()

    assert asyncio.run(run()) is None


def test_stale_value_survives_refresh_failure() -> None:
    async def run() -> tuple[int | None, int | None]:
        state = {"ok": True}

        def handler(request: httpx.Request) -> httpx.Response:
            if state["ok"]:
                return _rpc_response([100, 100, 100, 100, 100])
            raise httpx.ConnectError("rpc down")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = PriorityFeeProvider(
            rpc_url="http://rpc.test",
            client=client,
            min_lamports=0,
        )
        try:
            first = await provider.get_fee_lamports()
            # Simulate the cache expiring (60s > 30s refresh, < 300s stale TTL).
            provider._fee_fetched_at = time.monotonic() - 60.0
            state["ok"] = False
            second = await provider.get_fee_lamports()
        finally:
            await provider.close()
        return (first, second)

    first, second = asyncio.run(run())
    assert first == 100
    assert second == 100


def test_fee_clamped_to_min_and_max() -> None:
    async def run() -> tuple[int | None, int | None]:
        low_provider, _ = _provider([0, 0, 0, 0, 0], min_lamports=1_000)
        high_provider, _ = _provider([50_000_000] * 5, max_lamports=1_000_000)
        try:
            low = await low_provider.get_fee_lamports()
            high = await high_provider.get_fee_lamports()
        finally:
            await low_provider.close()
            await high_provider.close()
        return (low, high)

    low, high = asyncio.run(run())
    assert low == 1_000
    assert high == 1_000_000


def test_refresh_logs_fee_value() -> None:
    records: list[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    async def run() -> None:
        provider, _ = _provider([10, 20, 30, 40, 50])
        handler = _Handler()
        logger = logging.getLogger("priority_fee")
        old_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            await provider.get_fee_lamports()
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
            await provider.close()

    asyncio.run(run())
    assert any("PRIORITY_FEE lamports=40" in line for line in records)
