"""Research: Grok mention data for failed PumpFun coins (noise baseline) with temporal bucketing."""

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

PUMPFUN_URL = "https://frontend-api-v3.pump.fun/coins"
OUTPUT_PATH = "research/mt477_output/losers_mentions.json"
COIN_TARGET = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("research_losers")


def fetch_failed_coins() -> list[dict]:
    params = {"offset": 0, "limit": 50, "includeNsfw": "true"}
    resp = httpx.get(PUMPFUN_URL, params=params, timeout=15.0)
    resp.raise_for_status()
    data = resp.json()
    coins = data if isinstance(data, list) else data.get("coins", [])
    failed = []
    for c in coins:
        if not c.get("complete", True) and c.get("mint"):
            failed.append(c)
        if len(failed) == COIN_TARGET:
            break
    return failed


async def main():
    log.info("Fetching recent PumpFun launches...")
    coins = fetch_failed_coins()
    log.info("Found %d failed (non-graduated) coins", len(coins))

    if not coins:
        log.warning("No failed coins found")
        results: list[dict] = []
    else:
        results = []
        grok_calls = 0
        grok_successes = 0

        for i, coin in enumerate(coins, 1):
            symbol = coin.get("symbol", "?")
            mint = coin.get("mint", "")
            ts = coin.get("created_timestamp", 0)
            if isinstance(ts, (int, float)) and ts > 0:
                launched_at = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:
                launched_at = None

            log.info(
                "[%d/%d] %s (%s) launched %s — querying Grok...",
                i, len(coins), symbol, mint[:8],
                launched_at.isoformat() if launched_at else "unknown",
            )

            grok_calls += 1
            if launched_at:
                mention_data = await get_mentions_with_timestamps(symbol, mint, launched_at, hours=24)
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

            log.info("  -> mentions: total=%s, 0-5min=%s", mention_data.get("total_mentions"), mention_data.get("mentions_0_5min"))

            results.append({
                "ticker": symbol,
                "mint": mint,
                "launched_at": launched_at.isoformat() if launched_at else None,
                "graduated": False,
                **mention_data,
            })

            await asyncio.sleep(1.5)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved %d results to %s", len(results), OUTPUT_PATH)

    if results:
        success_rate = (grok_successes / grok_calls * 100) if grok_calls > 0 else 0
        total_mentions_vals = [r["total_mentions"] for r in results]
        mentions_0_5_vals = [r["mentions_0_5min"] for r in results if r["mentions_0_5min"] is not None]
        mentions_5_15_vals = [r["mentions_5_15min"] for r in results if r["mentions_5_15min"] is not None]

        avg_total = sum(total_mentions_vals) / len(total_mentions_vals) if total_mentions_vals else 0
        avg_0_5 = sum(mentions_0_5_vals) / len(mentions_0_5_vals) if mentions_0_5_vals else 0
        avg_5_15 = sum(mentions_5_15_vals) / len(mentions_5_15_vals) if mentions_5_15_vals else 0

        print()
        print("=" * 60)
        print(f"LOSERS — {len(results)} coins processed")
        print(f"Grok success rate: {success_rate:.1f}% ({grok_successes}/{grok_calls})")
        print(f"Average total mentions:  {avg_total:.1f}")
        print(f"Average mentions 0-5min: {avg_0_5:.1f}")
        print(f"Average mentions 5-15min: {avg_5_15:.1f}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
