import asyncio

import httpx

from src.risk.honeypot_simulation import HoneypotSimulationAdapter, HoneypotSimulationRequest


class RecordingProvider:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[str, HoneypotSimulationRequest]] = []

    async def __call__(self, backend: str, request: HoneypotSimulationRequest) -> object:
        self.calls.append((backend, request))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_successful_sell_simulation_passes() -> None:
    async def run() -> None:
        provider = RecordingProvider({"success": True})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="serialized-sell")
        )

        assert result.ok is True
        assert result.sell_simulation_passed is True
        assert result.blocked_reason is None
        assert result.provider_status == "ok"
        assert len(provider.calls) == 1

    asyncio.run(run())


def test_failed_sell_simulation_returns_blocked_result() -> None:
    async def run() -> None:
        provider = RecordingProvider({"success": False, "blockedReason": "sell blocked by program"})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="serialized-sell")
        )

        assert result.ok is True
        assert result.sell_simulation_passed is False
        assert result.blocked_reason == "sell blocked by program"
        assert result.provider_error is None

    asyncio.run(run())


def test_token_2022_transfer_hook_is_detected_from_fake_instruction_data() -> None:
    async def run() -> None:
        provider = RecordingProvider({"success": True})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(
                mint_address="mint",
                transaction_payload="serialized-sell",
                parsed_instructions=[
                    {
                        "program": "Token-2022",
                        "instruction": "ExecuteTransferHook",
                        "accounts": ["extraAccountMeta"],
                    }
                ],
            )
        )

        assert result.sell_simulation_passed is True
        assert result.transfer_hook_detected is True
        assert result.suspicious_instruction_detected is False

    asyncio.run(run())


def test_suspicious_conditional_sell_block_instruction_is_detected_from_fake_data() -> None:
    async def run() -> None:
        provider = RecordingProvider({"success": False, "blockedReason": "cooldown active"})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(
                mint_address="mint",
                transaction_payload="serialized-sell",
                parsed_instructions=[
                    {
                        "program": "custom-program",
                        "instruction": "ApplyMaxSellCooldown",
                        "notes": "blacklist before sell if wallet is flagged",
                    }
                ],
            )
        )

        assert result.sell_simulation_passed is False
        assert result.suspicious_instruction_detected is True
        assert result.blocked_reason == "cooldown active"

    asyncio.run(run())


def test_provider_timeout_and_error_degrade_gracefully() -> None:
    async def run_timeout() -> None:
        adapter = HoneypotSimulationAdapter(provider=RecordingProvider(httpx.TimeoutException("slow")))

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="serialized-sell")
        )

        assert result.ok is False
        assert result.sell_simulation_passed is False
        assert result.provider_status == "timeout"
        assert result.provider_error == "request timed out"

    async def run_error() -> None:
        adapter = HoneypotSimulationAdapter(provider=RecordingProvider(RuntimeError("boom")))

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="serialized-sell")
        )

        assert result.ok is False
        assert result.sell_simulation_passed is False
        assert result.provider_status == "provider_error"
        assert result.provider_error == "RuntimeError"

    asyncio.run(run_timeout())
    asyncio.run(run_error())


def test_malformed_response_and_missing_payload_degrade_gracefully() -> None:
    async def run_malformed() -> None:
        adapter = HoneypotSimulationAdapter(provider=RecordingProvider(["not", "a", "mapping"]))

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="serialized-sell")
        )

        assert result.ok is False
        assert result.provider_status == "malformed_payload"
        assert result.blocked_reason == "malformed response"

    async def run_missing_payload() -> None:
        provider = RecordingProvider({"success": True})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload="   ")
        )

        assert result.ok is False
        assert result.provider_status == "invalid_request"
        assert result.provider_error == "missing transaction payload"
        assert len(provider.calls) == 0

    asyncio.run(run_malformed())
    asyncio.run(run_missing_payload())


def test_no_live_network_calls() -> None:
    async def run() -> None:
        provider = RecordingProvider({"success": True, "result": {"source": "fake"}})
        adapter = HoneypotSimulationAdapter(provider=provider)

        result = await adapter.simulate_sell(
            HoneypotSimulationRequest(mint_address="mint", transaction_payload=b"unsigned-sell")
        )

        assert result.ok is True
        assert len(provider.calls) == 1
        _, request = provider.calls[0]
        assert request.transaction_payload == b"unsigned-sell"

    asyncio.run(run())
