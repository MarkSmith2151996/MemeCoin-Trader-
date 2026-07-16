"""Scrape DexScreener gainers, get launch dates, query Grok for day-1 mention counts."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

GROK_API_KEY = os.getenv("GROK_API_KEY")
GROK_API_URL = "https://api.x.ai/v1/responses"
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

OUTPUT_PATH = "research/mt473_output/winners_mentions.json"

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
    candidates = data.get("candidates", [])[:20]
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


def get_launch_date(mint: str) -> str | None:
    try:
        resp = httpx.get(f"{DEXSCREENER_TOKEN_URL}/{mint}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs", [])
        if not pairs:
            return None
        created_ats = [
            p.get("pairCreatedAt") for p in pairs if p.get("pairCreatedAt")
        ]
        if not created_ats:
            return None
        earliest = min(created_ats) / 1000
        dt = datetime.fromtimestamp(earliest, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception as exc:
        log.warning("Failed to get launch date for %s: %s", mint[:8], exc)
        return None


def query_grok_mentions(ticker: str, mint: str, launch_date: str) -> int | None:
    if not GROK_API_KEY:
        log.warning("GROK_API_KEY not set")
        return None

    prompt = (
        f"Search X for ${ticker} or {mint} posted on {launch_date}. "
        "Count how many unique accounts mentioned it. Reply with just the integer."
    )

    payload = {
        "model": "grok-4.3",
        "input": [{"role": "user", "content": prompt}],
        "tools": [{"type": "x_search"}],
    }

    try:
        resp = httpx.post(
            GROK_API_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {GROK_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        content_blocks = data.get("output", [])
        for block in content_blocks:
            inner = block.get("content") if isinstance(block, dict) else None
            if isinstance(inner, list):
                for item in inner:
                    text = item.get("output_text") or item.get("text") or ""
                    match = re.search(r"\d+", text.strip())
                    if match:
                        return int(match.group())
        log.warning("No numeric mention count found for %s (%s)", ticker, mint[:8])
        return None
    except Exception as exc:
        log.warning("Grok query failed for %s (%s): %s", ticker, mint[:8], exc)
        return None


def get_change_pct(candidate: dict) -> float | None:
    return candidate.get("change_24h_pct") or candidate.get("change_6h_pct") or candidate.get("change_1h_pct")


def main():
    candidates = scrape_gainers()
    results = []

    for i, c in enumerate(candidates):
        ticker = c["name"]
        pair = c.get("pair", "?")
        price_change = get_change_pct(c)
        log.info("[%d/%d] Processing %s (%s)...", i + 1, len(candidates), ticker, pair)

        mint = resolve_mint(ticker)
        if not mint:
            log.warning("  No mint found for %s, skipping", ticker)
            results.append({
                "ticker": ticker,
                "mint": None,
                "launch_date": None,
                "price_change_24h": price_change,
                "day1_mentions": None,
            })
            continue

        launch_date = get_launch_date(mint)
        if not launch_date:
            log.warning("  No launch date for %s (%s), skipping Grok query", ticker, mint[:8])
            results.append({
                "ticker": ticker,
                "mint": mint,
                "launch_date": None,
                "price_change_24h": price_change,
                "day1_mentions": None,
            })
            continue

        log.info("  Mint: %s, Launch: %s, Change: %s%%", mint[:12], launch_date, price_change)

        mentions = query_grok_mentions(ticker, mint, launch_date)
        log.info("  Day-1 mentions: %s", mentions)

        results.append({
            "ticker": ticker,
            "mint": mint,
            "launch_date": launch_date,
            "price_change_24h": price_change,
            "day1_mentions": mentions,
        })

        if i < len(candidates) - 1:
            time.sleep(2)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved results to %s", OUTPUT_PATH)

    print()
    print("=" * 80)
    print(f"{'TICKER':10s} {'LAUNCH DATE':14s} {'24H CHANGE':12s} {'MENTIONS':10s}")
    print("-" * 80)
    sorted_results = sorted(results, key=lambda r: r.get("day1_mentions") or 0, reverse=True)
    for r in sorted_results:
        chg = f"{r['price_change_24h']:+.1f}%" if r["price_change_24h"] is not None else "N/A"
        mentions = str(r["day1_mentions"]) if r["day1_mentions"] is not None else "N/A"
        launch = r["launch_date"] or "N/A"
        print(f"{r['ticker']:10s} {launch:14s} {chg:12s} {mentions:10s}")
    print("=" * 80)


if __name__ == "__main__":
    main()
