"""Unit coverage for the V2 collector's provider-to-candidate normalization."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from services import data_collector
from services.data_collector import (
    DataCollector,
    _strength_score,
    normalize_jupiter_token,
    normalize_pumpportal_token,
)


def test_jupiter_token_becomes_complete_candidate_record() -> None:
    observed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    token = {
        "id": "mintpump",
        "mcap": 10_000,
        "fdv": 12_000,
        "liquidity": 2_000,
        "usdPrice": 0.001,
        "holderCount": 42,
        "audit": {
            "mintAuthorityDisabled": True,
            "freezeAuthorityDisabled": True,
            "topHoldersPercentage": 37.5,
            "devBalancePercentage": 0,
        },
        "firstPool": {
            "id": "mintpump",
            "createdAt": (observed_at - timedelta(minutes=2)).isoformat(),
        },
        "stats1h": {"numBuys": 20, "numSells": 5, "buyVolume": 900, "sellVolume": 100},
        "stats5m": {"priceChange": 12.5},
    }

    candidate = normalize_jupiter_token(
        token,
        "jupiter_recent",
        sol_price_usd=200,
        observed_at=observed_at,
    )

    assert candidate is not None
    assert candidate["source"] == "jupiter_recent"
    assert candidate["pool_type"] == "bonding"
    assert candidate["age_seconds"] == 120
    assert candidate["corrected_age_seconds"] == 159
    assert candidate["pool_sol"] == 10
    assert candidate["price_sol"] == 0.000005
    assert candidate["strength_score"] == 88.0
    assert candidate["mint_authority_revoked"] is True
    assert candidate["freeze_authority_revoked"] is True
    assert candidate["top_holder_pct"] == 37.5
    assert candidate["creator_holdings_pct"] == 0
    assert candidate["raw_json"] is token


def test_pumpportal_new_token_is_preserved_when_gate_data_is_missing() -> None:
    candidate = normalize_pumpportal_token(
        {"mint": "freshpump", "vSolInBondingCurve": 7, "priceSol": 0.0001},
        sol_price_usd=200,
        observed_at=datetime(2026, 8, 26, tzinfo=UTC),
    )

    assert candidate is not None
    assert candidate["source"] == "pumpportal"
    assert candidate["pool_sol"] == 7
    assert candidate["volume_usd"] is None
    assert candidate["strength_score"] is None


def test_pumpportal_creator_self_buy_and_wallet_are_preserved() -> None:
    candidate = normalize_pumpportal_token(
        {
            "mint": "creatorpump",
            "traderPublicKey": "creator-wallet",
            "initialBuy": 3_000_000,
            "solAmount": 0.12,
            "vTokensInBondingCurve": 1_000_000_000,
        },
        sol_price_usd=200,
        observed_at=datetime(2026, 8, 26, tzinfo=UTC),
    )

    assert candidate is not None
    assert candidate["creator_wallet"] == "creator-wallet"
    assert candidate["creator_initial_buy"] == 3_000_000
    assert candidate["creator_initial_buy_sol"] == 0.12
    assert candidate["creator_self_snipe_pct"] == 3_000_000 / 1_003_000_000 * 100
    assert candidate["creator_prior_deploy_count"] is None
    assert candidate["creator_prior_rug_rate"] is None


def test_pumpportal_creator_history_is_cached_per_creator_day() -> None:
    class CreatorHistoryStore:
        def __init__(self) -> None:
            self.calls = 0

        async def get_creator_history(self, creator_wallet: str, as_of_date):
            self.calls += 1
            assert creator_wallet == "creator-wallet"
            assert as_of_date.isoformat() == "2026-08-26"
            return {"prior_deploy_count": 4, "prior_rug_rate": 0.25}

    async def run() -> None:
        store = CreatorHistoryStore()
        async with httpx.AsyncClient() as client:
            collector = DataCollector(store, client, api_key="test")
            candidate = {
                "creator_wallet": "creator-wallet",
                "observed_at": datetime(2026, 8, 26, 12, tzinfo=UTC),
            }
            await collector._attach_creator_history(candidate)
            await collector._attach_creator_history(candidate)

        assert candidate["creator_prior_deploy_count"] == 4
        assert candidate["creator_prior_rug_rate"] == 0.25
        assert store.calls == 1

    asyncio.run(run())


def test_pumpportal_listener_persists_creator_enriched_candidate(monkeypatch) -> None:
    class FakeSocket:
        def __init__(self) -> None:
            self._sent = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

        async def send(self, _message: str) -> None:
            return None

        def __aiter__(self):
            return self

        async def __anext__(self) -> str:
            if not self._sent:
                self._sent = True
                return json.dumps(
                    {
                        "mint": "listenerpump",
                        "traderPublicKey": "creator-wallet",
                        "initialBuy": 1,
                        "vTokensInBondingCurve": 99,
                    }
                )
            raise asyncio.CancelledError

    class Store:
        def __init__(self) -> None:
            self.candidates: list[dict] = []

        async def get_creator_history(self, _creator_wallet: str, _as_of_date):
            return {"prior_deploy_count": 2, "prior_rug_rate": 0.5}

        async def insert_candidate(self, candidate: dict) -> None:
            self.candidates.append(candidate)

    async def run() -> Store:
        store = Store()
        monkeypatch.setattr(
            data_collector.websockets,
            "connect",
            lambda *_args, **_kwargs: FakeSocket(),
        )
        async with httpx.AsyncClient() as client:
            collector = DataCollector(store, client, api_key="test")
            with pytest.raises(asyncio.CancelledError):
                await collector._listen_pumpportal()
        return store

    store = asyncio.run(run())
    assert store.candidates[0]["creator_wallet"] == "creator-wallet"
    assert store.candidates[0]["creator_prior_deploy_count"] == 2


def test_strength_score_uses_dollar_volume_ratio_not_transaction_ratio() -> None:
    sell_heavy_dollars = _strength_score(
        age_seconds=120,
        mcap_usd=10_000,
        volume_usd=1_000,
        buys=100,
        sells=1,
        buy_volume_usd=100,
        sell_volume_usd=900,
    )
    buy_heavy_dollars = _strength_score(
        age_seconds=120,
        mcap_usd=10_000,
        volume_usd=1_000,
        buys=1,
        sells=100,
        buy_volume_usd=900,
        sell_volume_usd=100,
    )

    assert sell_heavy_dollars == 50.2
    assert buy_heavy_dollars == 88.0
