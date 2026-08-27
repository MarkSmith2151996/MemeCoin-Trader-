"""Unit coverage for the V2 collector's provider-to-candidate normalization."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from services.data_collector import normalize_jupiter_token, normalize_pumpportal_token


def test_jupiter_token_becomes_complete_candidate_record() -> None:
    observed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    token = {
        "id": "mintpump",
        "mcap": 10_000,
        "fdv": 12_000,
        "liquidity": 2_000,
        "usdPrice": 0.001,
        "holderCount": 42,
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
    assert candidate["pool_sol"] == 10
    assert candidate["price_sol"] == 0.000005
    assert candidate["strength_score"] is not None
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
