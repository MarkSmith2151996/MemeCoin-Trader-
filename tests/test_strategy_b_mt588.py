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
    age_minutes: float = 5.0,
) -> dict:
    now_ms = int(time.time() * 1000)
    created_ms = now_ms - int(age_minutes * 60_000)
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
        "source_age_minutes": age_minutes,
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
            "creators": [{"isCreator": True, "pct": 0.0}],
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


def test_bonding_threshold_matches_graduated_after_mt593() -> None:
    # MT-593: walk-forward tuner found the score gate useful in only 1 of 3
    # iterations; the 55 bonding-curve threshold was filtering too aggressively
    # (937 FAIL:score in logs), so both thresholds are 40 now.
    assert MIN_SCORE_BONDING_CURVE == MIN_SCORE_GRADUATED == 40.0


# ── Slippage tiers ────────────────────────────────────────────────────


def test_slippage_tiers_by_pool_depth() -> None:
    assert _slippage_bps_for_pool(THICK_POOL_MIN_SOL + 1) == SLIPPAGE_BPS_THICK_POOL
    assert _slippage_bps_for_pool(15.0) == SLIPPAGE_BPS_MID_POOL
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
    sb._rugcheck_cache.clear()
    transport, _ = _transport(sol_price=sol_price)

    async def _fetch_rugcheck(client: httpx.AsyncClient, mint: str) -> httpx.Response:
        return _rugcheck_response()

    rugcheck = RugCheckClient(fetcher=_fetch_rugcheck)
    blocked_hours = sb.BLOCKED_UTC_HOURS
    blocked_weekdays = sb.BLOCKED_WEEKDAYS
    sb.BLOCKED_UTC_HOURS = frozenset()
    sb.BLOCKED_WEEKDAYS = frozenset()
    try:
        async with httpx.AsyncClient(transport=transport) as http:
            return await screen_coin(coin, http, rugcheck)
    finally:
        sb.BLOCKED_UTC_HOURS = blocked_hours
        sb.BLOCKED_WEEKDAYS = blocked_weekdays


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


def test_screen_coin_rejects_graduated_token_below_5_sol() -> None:
    # Raydium pool with 4 SOL depth. The MT-617 pool-mcap override fires
    # first: pool_mcap = 4 SOL * current SOL price * 4.4, so the $10K Jupiter mcap is
    # replaced and rejected below the $5,100 floor.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=4.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["mcap_pass"]
    assert f"${4.0 * SOL_PRICE * 4.4:.0f} < $5100 floor" in reason
    assert "jupiter_mcap=$10000" in reason


def test_screen_coin_accepts_bonding_curve_token_with_enough_depth() -> None:
    # Pool-consistent mcap: pool_mcap = 40 SOL * $75 * 2 = $6000, Jupiter mcap
    # $6000 is within 1.5x of pool_mcap so no override fires, the floor is met,
    # and the pool-depth floor passes. (Sub-5-SOL pools are now caught by the
    # MT-617 pool-mcap override before the pool-depth gate.)
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=6_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["liquidity_pass"]
    assert gates["mcap_pass"]


def test_screen_coin_rejects_bonding_curve_token_below_5_sol() -> None:
    # Bonding-curve pool at 4 SOL with an inflated Jupiter mcap: the MT-617
    # pool-mcap override caps mcap, rejecting it at the mcap gate.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=4.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["mcap_pass"]
    assert f"${4.0 * SOL_PRICE * 4.4:.0f} < $5100 floor" in reason


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
    # On-curve token with a weak signal (score ~34 < 40 after MT-593 lowered
    # the bonding threshold): passes liquidity but not the score threshold.
    # Pool is deep enough (100 SOL -> pool_mcap $15000) that the $20K Jupiter
    # mcap stays within 1.5x and the MT-617 override does not distort the
    # vol/mcap component.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=100.0 * SOL_PRICE,
        buys=5, sells=5, vol=300.0, mcap=20_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert gates["liquidity_pass"]
    assert not gates["score_pass"]
    assert "score=" in reason


def test_screen_coin_bonding_curve_mid_signal_now_passes_score_gate() -> None:
    # MT-593: the 55 bonding threshold was filtering too aggressively (937
    # FAIL:score); a mid signal (score ~45) that failed at 55 now passes at 40.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="abc123pump",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=7, sells=5, vol=500.0, mcap=25_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["score_pass"]


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
        buys=80, sells=5, vol=20_000.0, mcap=10_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["score_pass"]


# ── MT-593: creator-holdings gate (creator_pct > 0 rejects) ──────────


def _rugcheck_creator_response(creator_pct: float | None) -> httpx.Response:
    payload = {
        "mintAuthorityRevoked": True,
        "freezeAuthorityRevoked": True,
        "topHolders": [{"pct": 5.0}],
        "score": 100,
        "riskLevel": "safe",
    }
    if creator_pct is not None:
        payload["creators"] = [{"isCreator": True, "pct": creator_pct}]
    return httpx.Response(200, json=payload)


async def _screen_with_creator(
    creator_pct: float | None,
) -> tuple[bool, str, dict]:
    import scripts.run_strategy_b as sb

    sb._sol_price_cache = None
    sb._rugcheck_cache.clear()
    transport, _ = _transport(sol_price=SOL_PRICE)

    async def _fetch_rugcheck(client: httpx.AsyncClient, mint: str) -> httpx.Response:
        return _rugcheck_creator_response(creator_pct)

    rugcheck = RugCheckClient(fetcher=_fetch_rugcheck)
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    blocked_hours = sb.BLOCKED_UTC_HOURS
    blocked_weekdays = sb.BLOCKED_WEEKDAYS
    sb.BLOCKED_UTC_HOURS = frozenset()
    sb.BLOCKED_WEEKDAYS = frozenset()
    try:
        async with httpx.AsyncClient(transport=transport) as http:
            return await screen_coin(coin, http, rugcheck)
    finally:
        sb.BLOCKED_UTC_HOURS = blocked_hours
        sb.BLOCKED_WEEKDAYS = blocked_weekdays


def test_screen_coin_rejects_creator_still_holding() -> None:
    # MT-593: creator_holdings > 0 rejects — walk-forward selected 0.0 in all
    # 3 iterations. The old >10% gate (MT-588 fixture creator pct 2.0) would
    # have passed this token.
    passed, reason, gates = asyncio.run(_screen_with_creator(2.0))
    assert not passed
    assert not gates["creator_pass"]
    assert "creator_holdings>0" in reason


def test_screen_coin_passes_creator_with_zero_holdings() -> None:
    passed, _, gates = asyncio.run(_screen_with_creator(0.0))
    assert passed
    assert gates["creator_pass"]


def test_screen_coin_passes_creator_holdings_unavailable() -> None:
    # Missing creator data must not block the token (MT-593).
    passed, _, gates = asyncio.run(_screen_with_creator(None))
    assert passed
    assert gates["creator_pass"]


# ── MT-607: capacity backtest mcap floor ───────────────────────────────


def test_screen_coin_rejects_below_backtest_mcap_floor() -> None:
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=5_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["mcap_pass"]
    assert "$5000 < $5100 floor" in reason


# ── MT-617: corrected gate inputs (pool mcap / txn / age) ────────────


def test_mt617_pool_mcap_override_fires_on_inflated_mcap() -> None:
    # Jupiter reports a $10K mcap but the pool only holds 4 SOL: pool_mcap =
    # 4 * 75 * 2 = $600, so the reported mcap is >1.5x pool_mcap and the
    # override caps it at $600 -> rejected below the $5,100 floor.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=4.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["mcap_pass"]
    assert "jupiter_mcap=$10000" in reason


def test_mt617_pool_mcap_override_not_fired_within_ratio() -> None:
    # 40 SOL pool -> pool_mcap = $6000; a $6000 Jupiter mcap is within 1.5x
    # so no override: the reported mcap passes the gate unchanged.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=6_000.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["mcap_pass"]


def test_mt617_age_offset_applied_to_adjusted_txn_threshold() -> None:
    # A token at 3.5 Jupiter-minutes is corrected to 4.15 min: both land in
    # the same adjusted-txn bucket (8), so the txn threshold is unchanged.
    # 10 Jupiter txns * 1.24 = 12.4 still clears the 8-minimum.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=5, sells=5, vol=10_000.0, mcap=6_000.0,
        age_minutes=3.5,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["txn_pass"]


def test_mt617_age_offset_shifts_adjusted_txn_threshold() -> None:
    # 9 Jupiter txns * 1.24 = 11.16. At raw age 4.6 min the adjusted minimum
    # is 8 (pass); the +0.65 offset makes it 5.25 min -> minimum 12 (fail).
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=5, sells=4, vol=10_000.0, mcap=6_000.0,
        age_minutes=4.6,
    )
    passed, reason, gates = asyncio.run(_screen(coin))
    assert not passed
    assert not gates["txn_pass"]
    assert "txns=11.16<12" in reason


def test_mt617_txn_count_adjustment_applied() -> None:
    # 10 Jupiter txns * 1.24 = 12.4: at corrected age 5.65 min the adjusted
    # minimum is 12, so the corrected count passes the txn gate.
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=40.0 * SOL_PRICE,
        buys=5, sells=5, vol=10_000.0, mcap=6_000.0,
        age_minutes=5.0,
    )
    passed, _, gates = asyncio.run(_screen(coin))
    assert passed
    assert gates["txn_pass"]


def test_mt617_candidate_coin_gets_corrected_inputs() -> None:
    # screen_coin records the corrected mcap (when overridden) and the raw
    # Jupiter mcap alongside, WITHOUT mutating the raw field (the watch list
    # reuses the same coin dict across cycles).
    coin = _make_coin(
        mint="abc123pump",
        pool_id="raydiumPoolId123",
        liquidity_usd=4.0 * SOL_PRICE,
        buys=40, sells=5, vol=10_000.0, mcap=10_000.0,
    )
    passed, _, _ = asyncio.run(_screen(coin))
    assert not passed
    assert coin["mcap_corrected"] == 4.0 * SOL_PRICE * 4.4  # pool_mcap override stored
    assert coin["mcap_jupiter"] == 10_000.0
    assert coin["usd_market_cap"] == 10_000.0  # raw field untouched
