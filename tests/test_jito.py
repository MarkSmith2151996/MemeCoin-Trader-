import asyncio
import base64

import httpx

from src.core.config import load_settings
from src.chain.jito import JitoBlockEngineClient
from src.core.models import Side
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.jupiter_live import JupiterLiveExecutionAdapter
from src.execution.live_preflight import TransactionSimulationResult


class FakeResponse:
    def __init__(self, status_code: int, body) -> None:
        self.status_code = status_code
        self._body = body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class RecordingClient:
    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.calls: list[tuple[str, dict]] = []

    async def post(self, url: str, json: dict):
        self.calls.append((url, json))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class RecordingRpcSubmitter:
    def __init__(self, result: str | Exception = "rpc-signature-123") -> None:
        self.result = result
        self.calls: list[str | bytes] = []

    async def __call__(self, transaction: str | bytes) -> str:
        self.calls.append(transaction)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class RecordingBalanceLookup:
    def __init__(self, result: float | None | Exception) -> None:
        self.result = result
        self.calls = 0

    async def __call__(self) -> float | None:
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class RecordingSimulator:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[str | bytes] = []

    async def __call__(self, transaction: str | bytes) -> object:
        self.calls.append(transaction)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _armed_env(settings) -> dict[str, str]:
    return {
        "LIVE_TRADING_ENABLED": "true",
        "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
        "LIVE_KILL_SWITCH": "false",
        "PRIMARY_RPC_URL": "https://primary.example",
        "MAX_LIVE_TRADE_SOL": "0.01",
        "MAX_LIVE_DAILY_TRADES": "3",
        "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        "MIN_LIVE_WALLET_BALANCE_SOL": "0.05",
    }


def test_bundle_request_payload_construction() -> None:
    client = JitoBlockEngineClient(
        endpoint="https://jito.example/api/v1/bundles",
        http_client=RecordingClient(FakeResponse(200, {"result": "unused"})),
    )

    request = client.build_bundle_request(
        [b"raw-tx", "already-serialized"],
        tip_lamports=5_000,
        validator_tip_account="validator-abc",
    )

    assert request.endpoint == "https://jito.example/api/v1/bundles"
    assert request.transactions == [base64.b64encode(b"raw-tx").decode("ascii"), "already-serialized"]
    assert request.tip_lamports == 5_000
    assert request.payload == {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [
            [base64.b64encode(b"raw-tx").decode("ascii"), "already-serialized"],
            {
                "encoding": "base64",
                "tipLamports": 5_000,
                "validatorTipAccount": "validator-abc",
            },
        ],
    }


def test_successful_response_parsing() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(200, {"result": {"bundleId": "bundle-123"}}))
        client = JitoBlockEngineClient(endpoint="https://jito.example/api/v1/bundles", http_client=http_client)

        result = await client.submit_bundle(["tx-1"], tip_lamports=10_000)

        assert result.ok is True
        assert result.bundle_id == "bundle-123"
        assert result.error is None
        assert result.used_endpoint == "https://jito.example/api/v1/bundles"
        assert result.tip_lamports == 10_000
        assert len(http_client.calls) == 1

    asyncio.run(run())


def test_non_200_response_degrades_gracefully() -> None:
    async def run() -> None:
        client = JitoBlockEngineClient(http_client=RecordingClient(FakeResponse(503, {"error": "busy"})))

        result = await client.submit_bundle(["tx-1"])

        assert result.ok is False
        assert result.bundle_id is None
        assert result.error == "unexpected status: 503"
        assert result.status_code == 503

    asyncio.run(run())


def test_timeout_and_provider_exception_degrade_gracefully() -> None:
    async def run_timeout() -> None:
        client = JitoBlockEngineClient(http_client=RecordingClient(httpx.TimeoutException("slow")))

        result = await client.submit_bundle(["tx-1"])

        assert result.ok is False
        assert result.error == "request timed out"

    async def run_provider_error() -> None:
        client = JitoBlockEngineClient(http_client=RecordingClient(RuntimeError("boom")))

        result = await client.submit_bundle(["tx-1"])

        assert result.ok is False
        assert result.error == "provider exception: boom"

    asyncio.run(run_timeout())
    asyncio.run(run_provider_error())


def test_malformed_response_degrades_gracefully() -> None:
    async def run() -> None:
        bad_json_client = JitoBlockEngineClient(
            http_client=RecordingClient(FakeResponse(200, ValueError("bad json")))
        )
        missing_bundle_client = JitoBlockEngineClient(
            http_client=RecordingClient(FakeResponse(200, {"result": {"no_bundle": True}}))
        )

        bad_json_result = await bad_json_client.submit_bundle(["tx-1"])
        missing_bundle_result = await missing_bundle_client.submit_bundle(["tx-1"])

        assert bad_json_result.ok is False
        assert bad_json_result.error == "malformed json response"
        assert missing_bundle_result.ok is False
        assert missing_bundle_result.error == "missing bundle id"

    asyncio.run(run())


def test_no_wallet_or_private_key_required_and_no_real_network_calls() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(200, {"result": "bundle-456"}))
        client = JitoBlockEngineClient(http_client=http_client)

        result = await client.submit_bundle([b"unsigned-serialized-transaction"])

        assert result.ok is True
        assert len(http_client.calls) == 1
        used_url, used_payload = http_client.calls[0]
        assert used_url == client.endpoint
        assert "private" not in str(used_payload).lower()
        assert "secret" not in str(used_payload).lower()
        assert used_payload["method"] == "sendBundle"

    asyncio.run(run())


def test_live_adapter_default_behavior_is_unchanged_when_jito_disabled() -> None:
    async def run() -> None:
        rpc_submitter = RecordingRpcSubmitter("rpc-signature-disabled")
        adapter = JupiterLiveExecutionAdapter(rpc_submitter=rpc_submitter)

        result = await adapter.submit_serialized_swap("serialized-tx")

        assert result.ok is False
        assert result.provider == "guardrails"
        assert result.tx_signature is None
        assert "execution_mode_not_live" in result.diagnostics
        assert "live_confirmation_phrase_invalid" in result.diagnostics
        assert len(rpc_submitter.calls) == 0

        try:
            await adapter.execute_swap("mint", Side.BUY, 1.0)
        except NotImplementedError as exc:
            assert str(exc) == "Live swaps must be implemented behind risk gates"
        else:
            raise AssertionError("execute_swap should remain disabled")

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_calls_jito_when_explicitly_enabled() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(200, {"result": "bundle-789"}))
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            jito_enabled=True,
            jito_client=JitoBlockEngineClient(http_client=http_client),
            rpc_submitter=RecordingRpcSubmitter(),
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is True
        assert result.provider == "jito"
        assert result.jito_result is not None
        assert result.jito_result.bundle_id == "bundle-789"
        assert result.diagnostics == ["jito_attempted", "jito_bundle_submitted"]
        assert len(http_client.calls) == 1

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_handles_successful_jito_response() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(200, {"result": {"bundleId": "bundle-success"}}))
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            jito_enabled=True,
            jito_tip_lamports=5_000,
            jito_client=JitoBlockEngineClient(http_client=http_client),
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap(b"serialized-tx", amount_sol=0.01)

        assert result.ok is True
        assert result.provider == "jito"
        assert result.jito_result is not None
        assert result.jito_result.bundle_id == "bundle-success"
        assert result.jito_result.tip_lamports == 5_000
        assert result.tx_signature is None

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_falls_back_to_rpc_when_jito_fails_and_fallback_enabled() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(503, {"error": "busy"}))
        rpc_submitter = RecordingRpcSubmitter("rpc-signature-fallback")
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            jito_enabled=True,
            jito_fallback_to_rpc=True,
            jito_client=JitoBlockEngineClient(http_client=http_client),
            rpc_submitter=rpc_submitter,
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is True
        assert result.provider == "rpc"
        assert result.tx_signature == "rpc-signature-fallback"
        assert result.jito_result is not None
        assert result.jito_result.error == "unexpected status: 503"
        assert result.diagnostics == ["jito_attempted", "jito_failed_fallback_rpc"]
        assert len(rpc_submitter.calls) == 1

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_fails_safely_when_jito_fails_and_fallback_disabled() -> None:
    async def run() -> None:
        http_client = RecordingClient(FakeResponse(503, {"error": "busy"}))
        rpc_submitter = RecordingRpcSubmitter("rpc-should-not-run")
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            jito_enabled=True,
            jito_fallback_to_rpc=False,
            jito_client=JitoBlockEngineClient(http_client=http_client),
            rpc_submitter=rpc_submitter,
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "jito"
        assert result.tx_signature is None
        assert result.jito_result is not None
        assert result.jito_result.error == "unexpected status: 503"
        assert result.diagnostics == ["jito_attempted", "jito_failed_no_fallback"]
        assert len(rpc_submitter.calls) == 0

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_diagnostics_do_not_echo_confirmation_phrase_or_transaction() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env={
                "LIVE_TRADING_ENABLED": "true",
                "LIVE_CONFIRMATION_PHRASE": "wrong",
                "LIVE_KILL_SWITCH": "false",
                "MAX_LIVE_TRADE_SOL": "0.01",
                "MAX_LIVE_DAILY_TRADES": "3",
                "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
            },
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert "serialized-tx" not in str(result.diagnostics)
        assert settings.live_guardrails.confirmation_phrase not in str(result.diagnostics)

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_low_sol_balance_blocks_submission() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(0.02),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "preflight"
        assert result.error == "live preflight blocked submission"
        assert result.diagnostics == ["insufficient_wallet_balance"]

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_missing_balance_data_fails_closed() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(None),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "preflight"
        assert "wallet_balance_unknown" in result.diagnostics

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_simulation_failure_blocks_submission() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=False, error="simulation failed")),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "preflight"
        assert result.diagnostics == ["transaction_simulation_failed"]

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_preflight_success_passes_without_network_or_wallet_side_effects() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        rpc_submitter = RecordingRpcSubmitter("rpc-signature-preflight")
        balance_lookup = RecordingBalanceLookup(1.0)
        simulator = RecordingSimulator(TransactionSimulationResult(ok=True))
        adapter = JupiterLiveExecutionAdapter(
            rpc_submitter=rpc_submitter,
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=balance_lookup,
            transaction_simulator=simulator,
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is True
        assert result.provider == "rpc"
        assert balance_lookup.calls == 1
        assert simulator.calls == ["serialized-tx"]
        assert rpc_submitter.calls == ["serialized-tx"]

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_preflight_diagnostics_do_not_echo_transaction() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(0.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=False, error="bad simulation")),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert "serialized-tx" not in str(result.diagnostics)

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_primary_rpc_failure_can_use_backup_rpc() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={
                "execution": load_settings().execution.model_copy(
                    update={"mode": "live", "primary_rpc_url": "https://primary.example", "backup_rpc_url": "https://backup.example"}
                )
            }
        )
        primary_submitter = RecordingRpcSubmitter(RuntimeError("primary boom"))
        backup_submitter = RecordingRpcSubmitter("backup-signature")
        adapter = JupiterLiveExecutionAdapter(
            rpc_submitter=primary_submitter,
            backup_rpc_submitter=backup_submitter,
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is True
        assert result.provider == "rpc_backup"
        assert result.tx_signature == "backup-signature"
        assert result.diagnostics == ["jito_disabled", "rpc_primary_failed_backup_used"]
        assert primary_submitter.calls == ["serialized-tx"]
        assert backup_submitter.calls == ["serialized-tx"]

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_invalid_priority_fee_config_fails_closed() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env={**_armed_env(settings), "PRIORITY_FEE_LAMPORTS": "not-a-number", "PRIMARY_RPC_URL": "https://primary.example"},
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "config"
        assert "priority_fee_config_invalid" in result.diagnostics

        await adapter.close()

    asyncio.run(run())


def test_live_adapter_tripped_circuit_breaker_blocks_submission() -> None:
    async def run() -> None:
        settings = load_settings().model_copy(
            update={"execution": load_settings().execution.model_copy(update={"mode": "live"})}
        )
        breaker = LiveCircuitBreaker(rpc_failure_threshold=1)
        breaker.record_health_check(True)
        breaker.record_rpc_failure()
        adapter = JupiterLiveExecutionAdapter(
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=RecordingBalanceLookup(1.0),
            transaction_simulator=RecordingSimulator(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
        )

        result = await adapter.submit_serialized_swap("serialized-tx", amount_sol=0.01)

        assert result.ok is False
        assert result.provider == "circuit_breaker"
        assert "rpc_failure_threshold_reached" in result.diagnostics

        await adapter.close()

    asyncio.run(run())
