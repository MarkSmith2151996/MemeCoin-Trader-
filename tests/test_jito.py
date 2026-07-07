import asyncio
import base64

import httpx

from src.chain.jito import JitoBlockEngineClient


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
