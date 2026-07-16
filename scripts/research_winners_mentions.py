"""Scrape DexScreener gainers, get launch timestamps, query Grok for temporal mention data."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

from src.signals.grok_xsearch import get_mentions_with_timestamps

load_dotenv()

BROWSER_PC_URL = "http://172.21.32.1:8099/capture"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens"

GAINERS_URL = (
    "https://dexscreener.com/gainers"
    "?rankBy=priceChangeH24"
    "&order=desc"
    "&minLiq=250000"
    "&min24HTxns=300"
    "&min24HSells=30"
    "&min24HVol=100000"
)

OUTPUT_PATH = "research/mt477_output/winners_mentions.json"
COIN_TARGET = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("research_winners")


def scrape_gainers() -> list[dict]:
    log.info("Scraping gainers page...")
    resp = httpx.post(
        BROWSER_PC_URL,
        json={"url": GAINERS_URL, "wait": 6},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    candidates = data.get("candidates", [])
    log.info("Got %d candidates from gainers page", len(candidates))
    return candidates


def resolve_mint(ticker: str) -> str | None:
    try:
        resp = httpx.get(
            DEXSCREENER_SEARCH_URL,
            params={"q": ticker},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs", [])
        if not pairs:
            return None
        ticker_upper = ticker.upper()
        matching = [
            p for p in pairs
            if p.get("baseToken", {}).get("symbol", "").upper() == ticker_upper
        ]
        if not matching:
            return None
        best = max(matching, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        return best.get("baseToken", {}).get("address")
    except Exception as exc:
        log.warning("Failed to resolve mint for %s: %s", ticker, exc)
        return None


def get_token_info(mint: str) -> dict | None:
    """Fetch token info from DexScreener. Returns dict with launch, market cap, etc."""
    try:
        resp = httpx.get(f"{DEXSCREENER_TOKEN_URL}/{mint}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs", [])
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        created_ms = pair.get("pairCreatedAt")
        launched_at = None
        if created_ms:
            launched_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)

        info = {
            "launched_at": launched_at,
            "market_cap": pair.get("marketCap") or pair.get("fdv"),
            "liquidity_usd": (pair.get("liquidity") or {}).get("usd"),
            "txns": pair.get("txns", {}),
            "volume": pair.get("volume", {}),
            "price_usd": pair.get("priceUsd"),
            "price_change": pair.get("priceChange", {}),
        }
        return info
    except Exception as exc:
        log.warning("Failed to get token info for %s: %s", mint[:8], exc)
        return None


def get_change_pct(candidate: dict) -> float | None:
    return candidate.get("change_24h_pct") or candidate.get("change_6h_pct") or candidate.get("change_1h_pct")


async def main():
    candidates = scrape_gainers()
    results = []
    grok_calls = 0
    grok_successes = 0

    for i, c in enumerate(candidates):
        if len(results) >= COIN_TARGET:
            break

        ticker = c["name"]
        pair = c.get("pair", "?")
        price_change = get_change_pct(c)
        log.info("[%d/%d] Processing %s (%s)...", i + 1, len(candidates), ticker, pair)

        mint = resolve_mint(ticker)
        if not mint:
            log.warning("  No mint found for %s, skipping", ticker)
            continue

        token_info = get_token_info(mint)
        if not token_info:
            log.warning("  No token info for %s (%s), skipping", ticker, mint[:8])
            continue

        launched_at = token_info.get("launched_at")
        market_cap = token_info.get("market_cap")
        txns = token_info.get("txns", {})
        volume = token_info.get("volume", {})

        mcap_info = f"${market_cap:,.0f}" if market_cap else "N/A"
        log.info(
            "  Mint: %s, Market cap: %s, Launch: %s, Change: %s%%",
            mint[:12], mcap_info,
            launched_at.isoformat() if launched_at else "N/A",
            price_change,
        )

        grok_calls += 1
        if launched_at:
            mention_data = await get_mentions_with_timestamps(ticker, mint, launched_at, hours=24)
        else:
            mention_data = {
                "total_mentions": 0,
                "mentions_0_5min": None,
                "mentions_5_15min": None,
                "mentions_15_60min": None,
                "mentions_after_60min": None,
                "earliest_mention_min": None,
                "mention_timestamps": [],
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
            "market_cap": market_cap,
            "volume_24h": volume.get("h24") if volume else None,
            "txns_24h": txns.get("h24") if txns else None,
            **mention_data,
        })

        await asyncio.sleep(1.5)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved %d results to %s", len(results), OUTPUT_PATH)

    success_rate = (grok_successes / grok_calls * 100) if grok_calls > 0 else 0
    total_mentions_vals = [r["total_mentions"] for r in results]
    mentions_0_5_vals = [r["mentions_0_5min"] for r in results if r["mentions_0_5min"] is not None]
    mentions_5_15_vals = [r["mentions_5_15min"] for r in results if r["mentions_5_15min"] is not None]

    avg_total = sum(total_mentions_vals) / len(total_mentions_vals) if total_mentions_vals else 0
    avg_0_5 = sum(mentions_0_5_vals) / len(mentions_0_5_vals) if mentions_0_5_vals else 0
    avg_5_15 = sum(mentions_5_15_vals) / len(mentions_5_15_vals) if mentions_5_15_vals else 0

    print()
    print("=" * 60)
    print(f"WINNERS — {len(results)} coins processed")
    print(f"Grok success rate: {success_rate:.1f}% ({grok_successes}/{grok_calls})")
    print(f"Average total mentions:  {avg_total:.1f}")
    print(f"Average mentions 0-5min: {avg_0_5:.1f}")
    print(f"Average mentions 5-15min: {avg_5_15:.1f}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
