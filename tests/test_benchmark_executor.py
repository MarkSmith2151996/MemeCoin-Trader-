from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest


def _benchmark_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_executor.py"
    spec = importlib.util.spec_from_file_location("benchmark_executor", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_summarize_uses_nearest_rank_percentiles() -> None:
    benchmark = _benchmark_module()

    summary = benchmark._summarize([float(value) for value in range(1, 101)])

    assert summary.minimum_ms == 1.0
    assert summary.median_ms == 50.5
    assert summary.p95_ms == 95.0
    assert summary.p99_ms == 99.0
    assert summary.maximum_ms == 100.0


def test_simulation_request_uses_replace_blockhash_and_rejects_program_errors() -> None:
    benchmark = _benchmark_module()

    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert payload["method"] == "simulateTransaction"
            assert base64.b64decode(payload["params"][0]) == b"signed-transaction"
            assert payload["params"][1]["replaceRecentBlockhash"] is True
            return httpx.Response(200, json={"result": {"value": {"err": "AccountNotFound"}}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(RuntimeError, match="simulateTransaction returned an error"):
                await benchmark._simulate(client, "https://rpc.example", b"signed-transaction")

    asyncio.run(run())
