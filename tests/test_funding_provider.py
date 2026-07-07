import asyncio

import httpx

from src.risk.funding_provider import HeliusFundingProvider


def test_helius_provider_parses_inbound_sol_transfer() -> None:
    payload = [
        {
            "signature": "sig-1",
            "timestamp": 1_751_899_200,
            "nativeTransfers": [
                {
                    "fromUserAccount": "funder-1",
                    "toUserAccount": "buyer-1",
                    "amount": 2_500_000_000,
                },
                {
                    "fromUserAccount": "buyer-1",
                    "toUserAccount": "other-wallet",
                    "amount": 500_000_000,
                },
            ],
        }
    ]

    async def fake_fetcher(
        _client: httpx.AsyncClient,
        wallet_address: str,
        api_key: str,
        limit: int,
    ) -> httpx.Response:
        assert wallet_address == "buyer-1"
        assert api_key == "test-key"
        assert limit == 25
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", "https://example.test/helius"),
        )

    async def run() -> tuple:
        provider = HeliusFundingProvider(api_key="test-key", fetcher=fake_fetcher)
        lookup = await provider.lookup_wallet("buyer-1")
        transfers = await provider.get_recent_inbound_transfers("buyer-1")
        return lookup, transfers

    lookup, transfers = asyncio.run(run())

    assert lookup.provider_status == "ok"
    assert lookup.api_key_configured is True
    assert lookup.ignored_transfer_count == 1
    assert len(lookup.transfers) == 1
    assert transfers == lookup.transfers
    assert lookup.transfers[0].source_wallet == "funder-1"
    assert lookup.transfers[0].amount_sol == 2.5
    assert lookup.transfers[0].signature == "sig-1"


def test_helius_provider_missing_api_key_returns_graceful_unknown_result() -> None:
    fetch_calls = 0

    async def fake_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        nonlocal fetch_calls
        fetch_calls += 1
        return httpx.Response(200, json=[], request=httpx.Request("GET", "https://example.test/helius"))

    async def run() -> tuple:
        provider = HeliusFundingProvider(api_key="", fetcher=fake_fetcher)
        lookup = await provider.lookup_wallet("buyer-2")
        transfers = await provider.get_recent_inbound_transfers("buyer-2")
        return lookup, transfers

    lookup, transfers = asyncio.run(run())

    assert lookup.provider_status == "missing_api_key"
    assert lookup.api_key_configured is False
    assert lookup.error == "missing HELIUS_API_KEY"
    assert transfers is None
    assert fetch_calls == 0


def test_helius_provider_non_200_response_degrades_gracefully() -> None:
    async def fake_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        return httpx.Response(503, request=httpx.Request("GET", "https://example.test/helius"))

    async def run() -> tuple:
        provider = HeliusFundingProvider(api_key="test-key", fetcher=fake_fetcher)
        lookup = await provider.lookup_wallet("buyer-3")
        transfers = await provider.get_recent_inbound_transfers("buyer-3")
        return lookup, transfers

    lookup, transfers = asyncio.run(run())

    assert lookup.provider_status == "http_503"
    assert lookup.error == "non-200 response"
    assert transfers is None


def test_helius_provider_timeout_and_exception_degrade_gracefully() -> None:
    async def timeout_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    async def error_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        raise RuntimeError("boom")

    async def run(fetcher) -> tuple:
        provider = HeliusFundingProvider(api_key="test-key", fetcher=fetcher)
        lookup = await provider.lookup_wallet("buyer-4")
        transfers = await provider.get_recent_inbound_transfers("buyer-4")
        return lookup, transfers

    timeout_lookup, timeout_transfers = asyncio.run(run(timeout_fetcher))
    error_lookup, error_transfers = asyncio.run(run(error_fetcher))

    assert timeout_lookup.provider_status == "timeout"
    assert timeout_lookup.error == "request timed out"
    assert timeout_transfers is None
    assert error_lookup.provider_status == "provider_error"
    assert error_lookup.error == "RuntimeError"
    assert error_transfers is None


def test_helius_provider_malformed_json_degrades_gracefully() -> None:
    async def fake_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        return httpx.Response(
            200,
            text="not-json",
            request=httpx.Request("GET", "https://example.test/helius"),
        )

    async def run() -> tuple:
        provider = HeliusFundingProvider(api_key="test-key", fetcher=fake_fetcher)
        lookup = await provider.lookup_wallet("buyer-5")
        transfers = await provider.get_recent_inbound_transfers("buyer-5")
        return lookup, transfers

    lookup, transfers = asyncio.run(run())

    assert lookup.provider_status == "malformed_json"
    assert lookup.error == "response was not valid json"
    assert transfers is None


def test_helius_provider_ignores_irrelevant_and_non_inbound_transfers() -> None:
    payload = [
        {
            "signature": "sig-2",
            "timestamp": 1_751_899_260,
            "nativeTransfers": [
                {
                    "fromUserAccount": "buyer-6",
                    "toUserAccount": "elsewhere",
                    "amount": 2_000_000_000,
                },
                {
                    "fromUserAccount": "buyer-6",
                    "toUserAccount": "buyer-6",
                    "amount": 1_000_000_000,
                },
            ],
        },
        {
            "signature": "sig-3",
            "timestamp": 1_751_899_320,
            "description": "swap without native funding",
        },
    ]

    async def fake_fetcher(
        _client: httpx.AsyncClient,
        _wallet_address: str,
        _api_key: str,
        _limit: int,
    ) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", "https://example.test/helius"),
        )

    async def run() -> tuple:
        provider = HeliusFundingProvider(api_key="test-key", fetcher=fake_fetcher)
        lookup = await provider.lookup_wallet("buyer-6")
        transfers = await provider.get_recent_inbound_transfers("buyer-6")
        return lookup, transfers

    lookup, transfers = asyncio.run(run())

    assert lookup.provider_status == "ok"
    assert lookup.transfers == []
    assert transfers == []
    assert lookup.ignored_transfer_count == 3
