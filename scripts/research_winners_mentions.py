"""Research: find winner coins via DexScreener API, get temporal Grok mention data."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

from src.signals.grok_xsearch import get_mentions_with_timestamps

load_dotenv()

DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_BOOSTS_TOP = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_BOOSTS_LATEST = "https://api.dexscreener.com/token-boosts/latest/v1"

OUTPUT_PATH = "research/mt477_output/winners_mentions.json"
COIN_TARGET = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("research_winners")


def _collect_candidate_mints() -> set[str]:
    """Collect Solana token addresses from multiple DexScreener API sources."""
    mints: set[str] = set()

    # Source 1: token-boosts (trending)
    for url in (DEXSCREENER_BOOSTS_TOP, DEXSCREENER_BOOSTS_LATEST):
        try:
            resp = httpx.get(url, timeout=10)
            if resp.status_code == 200:
                boosts = resp.json()
                for b in boosts:
                    if b.get("chainId") == "solana":
                        addr = b.get("tokenAddress")
                        if addr:
                            mints.add(addr)
        except Exception as exc:
            log.warning("Failed to fetch boosts from %s: %s", url.rsplit("/", 1)[-1], exc)

    # Source 2: search query for broad terms
    search_terms = ["SOL", "pump", "raydium", "cat", "dog", "pepe", "moon", "ai", "trump", "coin", "bonk", "win", "baby", "elon", "woof", "meme", "doge", "shib", "pig", "bird", "fish", "wolf", "dragon", "lion", "bear", "bull"]
    for q in search_terms:
        try:
            resp = httpx.get(
                DEXSCREENER_SEARCH_URL,
                params={"q": q},
                timeout=8,
            )
            if resp.status_code == 200:
                data = resp.json()
                for p in data.get("pairs", []):
                    if p.get("chainId") == "solana":
                        addr = p.get("baseToken", {}).get("address")
                        if addr:
                            mints.add(addr)
        except Exception:
            pass

    log.info("Collected %d unique Solana token mints from DexScreener", len(mints))
    return mints


def _fetch_token_pairs(mint: str, client: httpx.Client) -> dict | None:
    """Fetch pair data and return best pair info, or None."""
    try:
        resp = client.get(f"{DEXSCREENER_TOKEN_URL}/{mint}", timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        pairs = data.get("pairs", [])
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return None
        best = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        mcap = float(best.get("marketCap") or 0)
        if mcap < 5000 or mcap > 500000:
            return None
        liq = float(best.get("liquidity", {}).get("usd", 0) or 0)
        if liq < 1000:
            return None
        price_change = float(best.get("priceChange", {}).get("h24", 0) or 0)
        symbol = best.get("baseToken", {}).get("symbol", "?")
        created_ms = best.get("pairCreatedAt")
        launched_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc) if created_ms else None
        return {
            "symbol": symbol,
            "mint": mint,
            "launched_at": launched_at,
            "market_cap": mcap,
            "liquidity_usd": liq,
            "volume_24h": float(best.get("volume", {}).get("h24", 0) or 0),
            "price_change_pct": price_change,
            "txns_24h": best.get("txns", {}).get("h24", {}),
        }
    except Exception:
        return None


async def main():
    mints = _collect_candidate_mints()

    winners: list[dict] = []
    with httpx.Client(timeout=10) as client:
        for mint in mints:
            info = _fetch_token_pairs(mint, client)
            if info:
                winners.append(info)

    winners.sort(key=lambda w: w["price_change_pct"], reverse=True)
    winners = winners[:COIN_TARGET]
    log.info("Selected %d winners (price change sorted)", len(winners))

    results = []
    grok_calls = 0
    grok_successes = 0

    for i, w in enumerate(winners):
        ticker = w["symbol"]
        mint = w["mint"]
        launched_at = w["launched_at"]
        price_change = w["price_change_pct"]

        log.info(
            "[%d/%d] %s (%s) — mcap=$%s, change=%+.1f%%",
            i + 1, len(winners), ticker, mint[:8],
            f"{w['market_cap']:,.0f}" if w.get("market_cap") else "N/A",
            price_change or 0,
        )

        grok_calls += 1
        if launched_at:
            mention_data = await get_mentions_with_timestamps(ticker, mint, launched_at, hours=24)
        else:
            mention_data = {
                "total_mentions": 0,
                "mentions_0_5min": None, "mentions_5_15min": None,
                "mentions_15_60min": None, "mentions_after_60min": None,
                "earliest_mention_min": None, "mention_timestamps": [],
            }

        if mention_data.get("total_mentions", 0) > 0:
            grok_successes += 1

        log.info(
            "  Mentions: total=%s, 0-5min=%s, 5-15min=%s",
            mention_data.get("total_mentions"),
            mention_data.get("mentions_0_5min"),
            mention_data.get("mentions_5_15min"),
        )

        results.append({
            "ticker": ticker,
            "mint": mint,
            "launched_at": launched_at.isoformat() if launched_at else None,
            "price_change_pct": price_change,
            "market_cap": w.get("market_cap"),
            "volume_24h": w.get("volume_24h"),
            **mention_data,
        })

        await asyncio.sleep(1.5)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved %d results to %s", len(results), OUTPUT_PATH)

    success_rate = (grok_successes / grok_calls * 100) if grok_calls > 0 else 0
    total_m = [r["total_mentions"] for r in results]
    m_0_5 = [r["mentions_0_5min"] for r in results if r["mentions_0_5min"] is not None]
    m_5_15 = [r["mentions_5_15min"] for r in results if r["mentions_5_15min"] is not None]

    print()
    print("=" * 60)
    print(f"WINNERS — {len(results)} coins processed")
    print(f"Grok success rate: {success_rate:.1f}% ({grok_successes}/{grok_calls})")
    print(f"Average total mentions:  {sum(total_m)/len(total_m):.1f}" if total_m else "N/A")
    print(f"Average mentions 0-5min: {sum(m_0_5)/len(m_0_5):.1f}" if m_0_5 else "N/A")
    print(f"Average mentions 5-15min: {sum(m_5_15)/len(m_5_15):.1f}" if m_5_15 else "N/A")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
