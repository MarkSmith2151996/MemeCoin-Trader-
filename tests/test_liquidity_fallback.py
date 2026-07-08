import asyncio

import httpx

from src.core.config import RiskConfig
from src.core.models import CheckResult, Signal, SignalSource, SignalType
from src.risk.liquidity import LiquidityProbe
from src.risk.scorer import DiscoveryRiskScorer


class FakeResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://example.invalid")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("boom", request=request, response=response)

    def json(self) -> object:
        return self._payload


class FakeAsyncClient:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    async def get(self, url: str) -> FakeResponse:
        self.calls.append(url)
        return self._responses[url]


def test_dexscreener_success_returns_without_jupiter_fallback() -> None:
    mint = "mint-success"
    dexscreener_url = f"https://dex.example/{mint}"
    jupiter_url = (
        f"https://jup.example?inputMint={mint}&outputMint=So11111111111111111111111111111111111111112"
        "&amount=1000000&slippageBps=1000"
    )
    client = FakeAsyncClient(
        {
            dexscreener_url: FakeResponse(
                200,
                {
                    "pairs": [
                        {
                            "chainId": "solana",
                            "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
                            "liquidity": {"quote": 42.0},
                        }
                    ]
                },
            )
        }
    )
    probe = LiquidityProbe(
        token_url_template="https://dex.example/{mint_address}",
        jupiter_quote_url="https://jup.example",
        client=client,
    )

    result = asyncio.run(probe.get_pool_info(mint))

    assert result == {"pool_liquidity_sol": 42.0, "source": "dexscreener"}
    assert client.calls == [dexscreener_url]
    assert jupiter_url not in client.calls


def test_dexscreener_failure_triggers_jupiter_fallback() -> None:
    mint = "mint-fallback"
    dexscreener_url = f"https://dex.example/{mint}"
    jupiter_url = (
        f"https://jup.example?inputMint={mint}&outputMint=So11111111111111111111111111111111111111112"
        "&amount=1000000&slippageBps=1000"
    )
    client = FakeAsyncClient(
        {
            dexscreener_url: FakeResponse(200, {"pairs": []}),
            jupiter_url: FakeResponse(200, {"routePlan": [{"swapInfo": {"poolLiquiditySol": 18.5}}], "outAmount": "900000000"}),
        }
    )
    probe = LiquidityProbe(
        token_url_template="https://dex.example/{mint_address}",
        jupiter_quote_url="https://jup.example",
        client=client,
    )

    result = asyncio.run(probe.get_pool_info(mint))

    assert result == {"pool_liquidity_sol": 18.5, "source": "jupiter_fallback"}
    assert client.calls == [dexscreener_url, jupiter_url]


def test_jupiter_fallback_can_derive_liquidity_from_valid_quote() -> None:
    mint = "mint-route"
    dexscreener_url = f"https://dex.example/{mint}"
    jupiter_url = (
        f"https://jup.example?inputMint={mint}&outputMint=So11111111111111111111111111111111111111112"
        "&amount=1000000&slippageBps=1000"
    )
    client = FakeAsyncClient(
        {
            dexscreener_url: FakeResponse(200, {"pairs": []}),
            jupiter_url: FakeResponse(200, {"routePlan": [{"swapInfo": {"label": "Raydium"}}], "outAmount": "2500000000"}),
        }
    )
    probe = LiquidityProbe(
        token_url_template="https://dex.example/{mint_address}",
        jupiter_quote_url="https://jup.example",
        client=client,
    )

    result = asyncio.run(probe.get_pool_info(mint))

    assert result == {"pool_liquidity_sol": 2.5, "source": "jupiter_fallback"}


def test_both_sources_failing_returns_unknown() -> None:
    mint = "mint-unknown"
    dexscreener_url = f"https://dex.example/{mint}"
    jupiter_url = (
        f"https://jup.example?inputMint={mint}&outputMint=So11111111111111111111111111111111111111112"
        "&amount=1000000&slippageBps=1000"
    )
    client = FakeAsyncClient(
        {
            dexscreener_url: FakeResponse(500, {}),
            jupiter_url: FakeResponse(200, {"routePlan": []}),
        }
    )
    probe = LiquidityProbe(
        token_url_template="https://dex.example/{mint_address}",
        jupiter_quote_url="https://jup.example",
        client=client,
    )

    result = asyncio.run(probe.get_pool_info(mint))

    assert result == {"pool_liquidity_sol": None, "source": "none"}


class StubLiquidityProbe:
    async def get_pool_info(self, mint_address: str) -> dict[str, object]:
        return {"pool_liquidity_sol": 12.0, "source": "jupiter_fallback"}


def test_discovery_risk_scorer_uses_fallback_liquidity_value() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="mint-scorer",
        payload={
            "uniqueBuyers": 25,
            "top10HolderPct": 30.0,
            "creatorHoldingPct": 5.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        liquidity_probe=StubLiquidityProbe(),
        enable_holder_lookup=False,
        enable_funding_analysis=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.token is not None
    assert assessment.token.liquidity_sol == 12.0
    assert assessment.liquidity_check == CheckResult.PASS
