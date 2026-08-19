"""MT-588 coverage: toptrending discovery, graduated-token gate, slippage
tiers, pool-depth floor, and screen_coin integration."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

import httpx

from scripts.run_strategy_b import (
    MID_POOL_MIN_SOL,
    MIN_SCORE_BONDING_CURVE,
    MIN_SCORE_GRADUATED,
    SLIPPAGE_BPS_MID_POOL,
    SLIPPAGE_BPS_THICK_POOL,
    THICK_POOL_MIN_SOL,
    _candidate_strength_score,
    _slippage_bps_for_pool,
    _token_graduation,
    fetch_candidates_jupiter,
    screen_coin,
)
from src.risk.rugcheck import RugCheckClient

SOL_PRICE = 75.0


def _token(mint: str, pool_id: str, launchpad: str = "pump.fun") -> dict:
    return {
        "id": mint,
        "symbol": "TST",
        "firstPool": {"id": pool_id, "createdAt": "2026-08-18T00:00:00Z"},
        "launchpad": launchpad,
    }


def _make_coin(
    *,
    mint: str = "abc123pump",
    pool_id: str | None = None,
    launchpad: str = "pump.fun",
    liquidity_usd: float = 10_000.0,
    mcap: float = 10_000.0,
    buys: int = 40,
    sells: int = 10,
    vol: float = 5_000.0,
) -> dict:
    now_ms = int(time.time() * 1000)
    created_ms = now_ms - 5 * 60_000
    return {
        "mint": mint,
        "ticker": "TST",
        "token_source": "pump",
        "token": _token(mint, pool_id or mint, launchpad),
        "usd_market_cap": mcap,
        "created_timestamp": created_ms,
        "volume": vol,
        "txns": buys + sells,
        "buy_sell_ratio": buys / max(sells, 1),
        "liquidity": liquidity_usd,
        "source_age_minutes": 5.0,
        "pair": {
            "chainId": "solana",
            "pairCreatedAt": created_ms,
            "baseToken": {"address": mint, "symbol": "TST"},
            "txns": {"h1": {"buys": buys, "sells": sells}},
            "volume": {"h1": vol},
            "liquidity": {"usd": liquidity_usd},
            "marketCap": mcap,
            "fdv": mcap,
            "priceUsd": "0.001",
            "priceChange": {"m5": 1.0, "h1": 2.0},
        },
    }


def _rugcheck_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "topHolders": [{"pct": 5.0}],
            "creators": [{"isCreator": True, "pct": 2.0}],
            "score": 100,
            "riskLevel": "safe",
        },
    )


def _transport(sol_price: float = SOL_PRICE):
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/search"):
            calls.append({"kind": "search", "query": request.url.params.get("query")})
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "So11111111111111111111111111111111111111112",
                        "symbol": "SOL",
                        "usdPrice": sol_price,
                    },
                ],
            )
        if path.endswith("/recent"):
            calls.append({"kind": "recent"})
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "deadbeefpump",
                        "symbol": "R1",
                        "firstPool": {
                            "id": "deadbeefpump",
                            "createdAt": datetime.now(UTC).isoformat(),
                        },
                        "launchpad": "pump.fun",
                        "liquidity": 2_000.0,
                        "mcap": 3_000.0,
                        "usdPrice": 0.001,
                        "stats1h": {"numBuys": 5, "numSells": 2,
                                   "buyVolume": 300.0, "sellVolume": 200.0},
                        "stats5m": {},
                    },
                ],
            )
        calls.append({"kind": path})
        return httpx.Response(200, json=[])

    return httpx.MockTransport(handler), calls


# ── Graduation classification ─────────────────────────────────────────


def test_graduation_bonding_curve_when_pool_is_the_mint() -> None:
    assert _token_graduation(_token("abc123pump", "abc123pump")) == "bonding"


def test_graduation_pumpswap_pool_is_graduated() -> None:
    # PumpSwap pools end in "pump" but differ from the mint.
    assert _token_graduation(_token("abc123pump", "zzz999pump")) == "graduated"


def test_graduation_raydium_pool_is_graduated() -> None:
    assert _token_graduation(_token("abc123pump", "raydiumPoolId123")) == "graduated"


def test_graduation_non_pump_launch_is_graduated() -> None:
    assert _token_graduation(_token("meteoramint456", "meteoramint456", "met-dbc")) == "graduated"


def test_graduation_missing_token_defaults_to_graduated() -> None:
    assert _token_graduation(None) == "graduated"


def test_graduation_missing_pool_id_is_bonding_for_pump_mint() -> None:
    assert _token_graduation({"id": "abc123pump", "firstPool": {}}) == "bonding"


# ── Strength score ────────────────────────────────────────────────────


def test_strength_score_rises_with_activity() -> None:
    weak = _make_coin(buys=4, sells=2, vol=300.0, mcap=5_000.0)
    strong = _make_coin(buys=40, sells=5, vol=10_000.0, mcap=5_000.0)
    assert _candidate_strength_score(strong, 5.0) > _candidate_strength_score(weak, 5.0)


def test_strength_score_is_bounded_at_100() -> None:
    coin = _make_coin(buys=1000, sells=1, vol=1_000_000.0, mcap=5_000.0)
    assert _candidate_strength_score(coin, 5.0) <= 100.0


def test_bonding_threshold_is_higher_than_graduated() -> None:
    assert MIN_SCORE_BONDING_CURVE > MIN_SCORE_GRADUATED


# ── Slippage tiers ────────────────────────────────────────────────────


def test_slippage_tiers_by_pool_depth() -> None:
    assert _slippage_bps_for_pool(THICK_POOL_MIN_SOL + 1) == SLIPPAGE_BPS_THICK_POOL
    assert _slippage_bps_for_pool(30.0) == SLIPPAGE_BPS_MID_POOL
    assert _slippage_bps_for_pool(MID_POOL_MIN_SOL) == SLIPPAGE_BPS_MID_POOL
    assert _slippage_bps_for_pool(MID_POOL_MIN_SOL - 0.01) is None
    assert _slippage_bps_for_pool(None) is None


# ── Discovery: three endpoints, dedup by mint ────────────────────────


def test_fetch_candidates_jupiter_calls_three_endpoints() -> None:
    transport, calls = _transport()

    async def run() -> list[dict]:
        async with httpx.AsyncClient(transport=transport) as http:
            return await fetch_candidates_jupiter(http)

    candidates = asyncio.run(run())
    kinds = [c.get("kind") for c in calls]
    assert any(k.endswith("/toporganicscore/5m") for k in kinds)
    assert "recent" in kinds
    assert any(k.endswith("/toptrending/5m") for k in kinds)
    assert len(candidates) == 1  # recent mint deduped against the other endpoints
    assert candidates[0]["mint"] == "deadbeefpump"
    assert "token" in candidates[0]


def test_fetch_candidates_jupiter_dedupes_across_endpoints() -> None:
    transport, calls = _transport()

    async def run() -> list[dict]:
        async with httpx.AsyncClient(transport=transport) as http:
            return await fetch_candidates_jupiter(http)

    asyncio.run(run())
    assert sum(1 for c in calls if c.get("kind") == "recent") == 1


# ── Pool-depth floor in screen_coin ──────────────────────────────────


async def _screen(coin: dict, sol_price: float = SOL_PRICE) -> tuple[bool, str, dict]:
    # Keep tests self-contained: drop any SOL price cached by a prior test.
    import scripts.run_strategy_b as sb

    sb._sol_price_cache = None
    transport, _ = _transport(sol_price=sol_price)

    async def _fetch_rugcheck(client: httpx.AsyncClient, mint: str) -> httpx.Response:
        return _rugcheck_response()

    rugcheck = RugCheckClient(fetcher=_fetch_rugcheck)
    async with httpx.AsyncClient(transport=transport) as http:
        return await screen_coin(coin, http, rugcheck)


def test_screen_coin_accepts_graduated_token_with_enough_depth() -> None:
    # Raydium pool, 100 SOL depth, strong activity -> normal (lower) score gate.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=100.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["liquidity_pass"]
    assert gates["score_pass"]


def test_screen_coin_rejects_graduated_token_below_50_sol() -> None:
    # Raydium pool with 40 SOL depth -> below the 50 SOL graduated floor.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["liquidity_pass"]
    assert "pool_depth=40.0SOL<50SOL" in reason


def test_screen_coin_accepts_bonding_curve_token_at_30_sol() -> None:
    # Bonding-curve pool at exactly 30 SOL -> passes the 30 SOL floor.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=30.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["liquidity_pass"]


def test_screen_coin_rejects_bonding_curve_token_below_30_sol() -> None:
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=20.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["liquidity_pass"]
    assert "pool_depth=20.0SOL<30SOL" in reason


def test_screen_coin_skips_token_without_liquidity_data() -> None:
    coin = _make_coin(liquidity_usd=0.0, buys=40, sells=5, vol=10_000.0)
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert "no_pool_liquidity" in reason
    assert not gates["liquidity_pass"]


def test_screen_coin_skips_token_when_sol_price_unavailable() -> None:
    coin = _make_coin(liquidity_usd=10_000.0, buys=40, sells=5, vol=10_000.0)
    passed, reason, _ = asyncio.run(_screen(coin, sol_price=0.0))
    assert not passed
    assert "no_pool_liquidity" in reason


# ── Graduated gate: bonding-curve tokens need a stronger signal ──────


def test_screen_coin_bonding_curve_weak_signal_fails_score_gate() -> None:
    # On-curve token with a middling signal: passes liquidity but not the
    # higher bonding-curve score threshold.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=7, sells=5, vol=500.0, mcap=25_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert gates["liquidity_pass"]
    assert not gates["score_pass"]
    assert "score=" in reason


def test_screen_coin_graduated_same_signal_passes_score_gate() -> None:
    # Same weak-ish signal on a graduated (Raydium) pool passes the normal
    # score threshold — this is the graduated-token preference.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=60.0 * SOL_PRICE,
        buys=7, sells=5, vol=500.0, mcap=25_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["score_pass"]


def test_screen_coin_bonding_curve_strong_signal_passes() -> None:
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=80, sells=5, vol=20_000.0, mcap=5_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["score_pass"]
