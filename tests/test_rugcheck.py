import asyncio

import httpx

from src.risk.rugcheck import RugCheckClient


def test_rugcheck_parses_successful_response() -> None:
    payload = {
        "tokenMeta": {
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": False,
        },
        "topHolders": [
            {"address": "holder-1", "pct": 12.5},
            {"address": "holder-2", "pct": 8.25},
        ],
        "liquidityLocked": True,
        "liquidityStatus": "locked",
        "isHoneypot": False,
        "riskScore": 27,
        "riskLevel": "low",
    }

    async def fake_fetcher(_client: httpx.AsyncClient, mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", f"https://example.test/{mint_address}"),
        )

    async def run():
        client = RugCheckClient(fetcher=fake_fetcher)
        return await client.fetch_report("mint-1")

    result = asyncio.run(run())

    assert result.mint_address == "mint-1"
    assert result.found is True
    assert result.provider_status == "ok"
    assert result.mint_authority_revoked is True
    assert result.freeze_authority_revoked is False
    assert result.top_holder_pct == 20.75
    assert result.liquidity_locked is True
    assert result.liquidity_status == "locked"
    assert result.is_honeypot is False
    assert result.risk_score == 27.0
    assert result.risk_level == "low"


def test_rugcheck_missing_optional_fields_degrade_to_unknowns() -> None:
    payload = {"tokenMeta": {}, "verification": {}}

    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    async def run():
        client = RugCheckClient(fetcher=fake_fetcher)
        return await client.fetch_report("mint-unknown")

    result = asyncio.run(run())

    assert result.found is True
    assert result.provider_status == "ok"
    assert result.mint_authority_revoked is None
    assert result.freeze_authority_revoked is None
    assert result.top_holder_pct is None
    assert result.liquidity_locked is None
    assert result.liquidity_status is None
    assert result.is_honeypot is None
    assert result.risk_score is None
    assert result.risk_level is None


def test_rugcheck_present_null_mint_and_freeze_authorities_are_revoked() -> None:
    payload = {
        "token": {
            "mintAuthority": None,
            "freezeAuthority": None,
        },
        "tokenMeta": {
            "updateAuthority": "unrelated-authority",
        },
    }

    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    result = asyncio.run(RugCheckClient(fetcher=fake_fetcher).fetch_report("mint-null-authorities"))

    assert result.mint_authority_revoked is True
    assert result.freeze_authority_revoked is True


def test_rugcheck_unrelated_authorities_do_not_imply_mint_or_freeze_state() -> None:
    payload = {
        "tokenMeta": {
            "updateAuthority": "unrelated-authority",
        },
        "token_extensions": {
            "metadataPointer": {"authority": "unrelated-authority"},
        },
    }

    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    result = asyncio.run(RugCheckClient(fetcher=fake_fetcher).fetch_report("mint-unrelated-authorities"))

    assert result.mint_authority_revoked is None
    assert result.freeze_authority_revoked is None


def test_rugcheck_non_200_response_degrades_gracefully() -> None:
    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            503,
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    async def run():
        client = RugCheckClient(fetcher=fake_fetcher)
        return await client.fetch_report("mint-down")

    result = asyncio.run(run())

    assert result.found is False
    assert result.provider_status == "http_503"
    assert result.error == "non-200 response"


def test_rugcheck_timeout_and_provider_exceptions_degrade_gracefully() -> None:
    async def timeout_fetcher(
        _client: httpx.AsyncClient,
        _mint_address: str,
    ) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    async def error_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        raise RuntimeError("boom")

    async def run(fetcher):
        client = RugCheckClient(fetcher=fetcher)
        return await client.fetch_report("mint-error")

    timeout_result = asyncio.run(run(timeout_fetcher))
    error_result = asyncio.run(run(error_fetcher))

    assert timeout_result.found is False
    assert timeout_result.provider_status == "timeout"
    assert timeout_result.error == "request timed out"
    assert error_result.found is False
    assert error_result.provider_status == "provider_error"
    assert error_result.error == "RuntimeError"


def test_rugcheck_malformed_json_degrades_gracefully() -> None:
    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            text="not-json",
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    async def run():
        client = RugCheckClient(fetcher=fake_fetcher)
        return await client.fetch_report("mint-bad-json")

    result = asyncio.run(run())

    assert result.found is False
    assert result.provider_status == "malformed_json"
    assert result.error == "response was not valid json"


def test_rugcheck_non_object_payload_degrades_gracefully() -> None:
    async def fake_fetcher(_client: httpx.AsyncClient, _mint_address: str) -> httpx.Response:
        return httpx.Response(
            200,
            json=["not", "an", "object"],
            request=httpx.Request("GET", "https://example.test/mint"),
        )

    async def run():
        client = RugCheckClient(fetcher=fake_fetcher)
        return await client.fetch_report("mint-array")

    result = asyncio.run(run())

    assert result.found is False
    assert result.provider_status == "malformed_payload"
    assert result.error == "response payload was not an object"
