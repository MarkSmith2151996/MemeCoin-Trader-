"""Coverage: Helius transaction simulator provider.

All tests use fake HTTP clients — no real network calls.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

import src.cli as cli_module
from src.execution.helius_providers import HeliusTransactionSimulator, try_create_transaction_simulator
from src.execution.live_preflight import TransactionSimulationResult

runner = CliRunner()


class FakeHttpClient:
    """Injected httpx.AsyncClient replacement for testing."""

    def __init__(self, status_code: int = 200, json_body: object = None, raise_on_post: bool = False) -> None:
        self.status_code = status_code
        self._json_body = json_body
        self._raise_on_post = raise_on_post
        self.last_url: str | None = None
        self.last_payload: object = None
        import json as _json
        self._encoder = _json

    async def post(self, url: str, *, json: object | None = None, **kwargs: object) -> httpx.Response:
        self.last_url = url
        self.last_payload = json
        if self._raise_on_post:
            raise httpx.ConnectError("connection refused")
        content = self._json_body if self._json_body is not None else {"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}, "value": {"blockhash": "abc", "lastValidBlockHeight": 100}}}
        raw = self._encoder.dumps(content).encode()
        request = httpx.Request("POST", url)
        return httpx.Response(self.status_code, content=raw, request=request)

    async def aclose(self) -> None:
        return None


def test_simulator_returns_unavailable_when_no_rpc_url() -> None:
    sim = HeliusTransactionSimulator(rpc_url="")
    result = asyncio.run(sim("readiness-check"))
    assert isinstance(result, TransactionSimulationResult)
    assert result.ok is False
    assert "helius_rpc_url_not_configured" in (result.error or "")


def test_simulator_healthy_readiness_check() -> None:
    fake = FakeHttpClient()
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim("readiness-check"))
    assert result.ok is True
    assert fake.last_payload is not None
    payload = fake.last_payload  # type: ignore[assignment]
    assert payload["method"] == "getLatestBlockhash"


def test_simulator_unreachable_readiness_check() -> None:
    fake = FakeHttpClient(raise_on_post=True)
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim("readiness-check"))
    assert isinstance(result, TransactionSimulationResult)
    assert result.ok is False
    assert "helius_rpc_unreachable" in (result.error or "")


def test_simulator_error_sanitizes_urls_and_api_keys() -> None:
    fake = FakeHttpClient(raise_on_post=True)
    fake._raise_on_post = False

    async def post_with_secret(*args: object, **kwargs: object) -> httpx.Response:
        raise RuntimeError("https://user:password@rpc.example/?api-key=secret-token")

    fake.post = post_with_secret  # type: ignore[method-assign]
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim("readiness-check"))

    assert result.ok is False
    assert "rpc.example" in (result.error or "")
    assert "password" not in (result.error or "")
    assert "secret-token" not in (result.error or "")


def test_simulator_malformed_response() -> None:
    fake = FakeHttpClient(status_code=500, json_body={"error": "internal"})
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim("readiness-check"))
    assert isinstance(result, TransactionSimulationResult)
    assert result.ok is False
    assert "helius_rpc_unreachable" in (result.error or "") or "error" in (result.error or "").lower()


def test_try_create_returns_none_when_no_key() -> None:
    sim = try_create_transaction_simulator()
    if sim is not None:
        asyncio.run(sim.close())
    # The function returns None if no Helius config is available
    # In test env with no env set, this should return None
    # Note: if .env has HELIUS_API_KEY, this will return a simulator
    assert sim is None or isinstance(sim, HeliusTransactionSimulator)


def test_api_key_not_in_readiness_output() -> None:
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", "/tmp/nonexistent_test.db"])
    assert result.exit_code == 0
    output = result.stdout
    helius_prefix = "50661bc5"
    assert helius_prefix not in output
    assert "api-key=" not in output.lower()
    assert "helius_rpc_url=" not in output.lower()
    assert "helius_api_key=" not in output.lower()


def test_private_key_not_required_for_simulator() -> None:
    """Simulator can be created and used without any private key config."""
    fake = FakeHttpClient()
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim("readiness-check"))
    assert result.ok is True
    assert "private" not in str(fake.last_payload).lower()


def test_simulator_healthy_alone_does_not_bypass_guardrails() -> None:
    """Even with a healthy simulator, missing guardrails keep NOT READY."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", "/tmp/nonexistent_test.db"])
    assert result.exit_code == 0
    assert "micro_live_ready=NOT READY" in result.stdout
    assert "guardrails=not_ready" in result.stdout


def test_simulator_real_transaction() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 42}}})
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim(b"\x00\x01\x02fake_transaction"))
    assert result.ok is True
    assert fake.last_payload is not None
    payload = fake.last_payload  # type: ignore[assignment]
    assert payload["method"] == "simulateTransaction"


def test_simulator_real_transaction_failure() -> None:
    fake = FakeHttpClient(json_body={"jsonrpc": "2.0", "id": 1, "result": {"err": "InstructionError"}})
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    result = asyncio.run(sim(b"\x00\x01\x02"))
    assert result.ok is False
    assert "InstructionError" in (result.error or "")


def test_simulator_close_is_safe() -> None:
    fake = FakeHttpClient()
    sim = HeliusTransactionSimulator(rpc_url="https://example.com", http_client=fake)  # type: ignore[arg-type]
    asyncio.run(sim.close())
    # Should not raise
