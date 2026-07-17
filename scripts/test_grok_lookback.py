"""Test: query Grok lookback windows for a curated set of newer and older coins.

Usage:
    python3 scripts/test_grok_lookback.py

Saves results to research/mt479_output/grok_lookback_results.json.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.grok_xsearch import get_mentions_with_timestamps

load_dotenv()

DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
OUTPUT_PATH = "research/mt479_output/grok_lookback_results.json"

COINS = [
    ("CXMT", "~53min"),
    ("TRUMP", "~1h"),
    ("BRIAN", "~3h"),
    ("TENEO", "~5h"),
    ("OPENAI", "~12h"),
    ("NVDA.O", "~14h"),
    ("OpenAI", "~20h"),
    ("Index", "~21h"),
    ("T.My", "~22h"),
    ("AnsemCat", "~1d"),
    ("HOOD", "~13h"),
    ("HAN", "~1d"),
    ("CASHCAT", "~1d"),
    ("BoLe", "~1d"),
    ("PONS", "~3d"),
    ("WOOD", "~3d"),
    ("HOODIO", "~5d"),
    ("SKHY", "~6d"),
    ("CASHCAT", "~8d"),
    ("HOODIE", "~8d"),
]

HOURS_WINDOWS = [1, 6, 12, 24, 72, 168]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("grok_lookback")


def parse_age_hours(age_str: str) -> float | None:
    s = age_str.strip("~")
    if s.endswith("min"):
        return float(s.replace("min", "")) / 60
    elif s.endswith("h"):
        return float(s.replace("h", ""))
    elif s.endswith("d"):
        return float(s.replace("d", "")) * 24
    return None


async def resolve_coin(
    ticker: str,
    age_str: str,
    http: httpx.AsyncClient,
) -> dict | None:
    """Search DexScreener and find the Solana pair matching expected age."""
    target_age_h = parse_age_hours(age_str)
    if target_age_h is None:
        log.warning("Cannot parse age '%s' for %s", age_str, ticker)
        return None

    try:
        resp = await http.get(
            DEXSCREENER_SEARCH,
            params={"q": ticker},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs") or []
    except Exception as exc:
        log.warning("DexScreener search failed for %s: %s", ticker, exc)
        return None

    now = datetime.now(timezone.utc)
    best = None
    best_age_diff = float("inf")
    candidates = []

    for p in pairs:
        if not isinstance(p, dict) or p.get("chainId") != "solana":
            continue
        created_ms = p.get("pairCreatedAt")
        if not isinstance(created_ms, (int, float)):
            continue
        launched = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
        pair_age_h = (now - launched).total_seconds() / 3600
        symbol = (p.get("baseToken") or {}).get("symbol", "")
        mint = (p.get("baseToken") or {}).get("address", "")
        if not mint:
            continue
        age_diff = abs(pair_age_h - target_age_h)
        candidates.append((symbol, mint, created_ms, age_diff, pair_age_h))

    # Prefer exact ticker match, then closest age
    exact = [c for c in candidates if c[0].upper() == ticker.upper()]
    pool = exact if exact else candidates

    for symbol, mint, created_ms, age_diff, pair_age_h in pool:
        if age_diff < best_age_diff:
            best_age_diff = age_diff
            best = {
                "mint": mint,
                "pairCreatedAt": created_ms,
                "symbol": symbol,
                "ticker": ticker,
                "pair_age_h": pair_age_h,
            }

    if best and best_age_diff > 12:
        log.warning(
            "Age mismatch for %s: %.1fh diff (pair=%.1fh vs expected=%.1fh)",
            ticker, best_age_diff, best["pair_age_h"], target_age_h,
        )
    return best


async def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    results = []

    async with httpx.AsyncClient() as http:
        for ticker, age_str in COINS:
            log.info("Resolving %s (%s)...", ticker, age_str)
            info = await resolve_coin(ticker, age_str, http)

            if not info:
                log.warning("Could not resolve %s", ticker)
                results.append({
                    "ticker": ticker,
                    "age_str": age_str,
                    "mint": None,
                    "error": "not_resolved",
                })
                continue

            mint = info["mint"]
            created_ms = info["pairCreatedAt"]
            launched_at = datetime.fromtimestamp(
                created_ms / 1000, tz=timezone.utc,
            )

            row: dict = {
                "ticker": ticker,
                "age_str": age_str,
                "age_hours": parse_age_hours(age_str),
                "mint": mint,
                "symbol": info["symbol"],
                "pairCreatedAt": created_ms,
                "launched_at": launched_at.isoformat(),
            }

            all_timestamps: list[str] = []
            for h in HOURS_WINDOWS:
                log.info("  Grok %s %sh window...", ticker, h)
                mention_data = await get_mentions_with_timestamps(
                    ticker, mint, launched_at, hours=h,
                )
                total = mention_data.get("total_mentions", 0)
                row[f"mentions_{h}h"] = total
                tss = mention_data.get("mention_timestamps", [])
                if isinstance(tss, list):
                    all_timestamps.extend(tss)
                await asyncio.sleep(2)

            earliest = min(all_timestamps) if all_timestamps else None
            row["earliest_tweet"] = earliest
            results.append(row)

    # Print results table
    header = f"{'TICKER':<12} {'AGE':<10} {'1h':<5} {'6h':<5} {'12h':<5} {'24h':<5} {'3d':<5} {'7d':<5}  EARLIEST_TWEET"
    print(f"\n{header}")
    print("-" * len(header))

    for r in results:
        if r.get("mint"):
            h1 = r.get("mentions_1h", 0)
            h6 = r.get("mentions_6h", 0)
            h12 = r.get("mentions_12h", 0)
            h24 = r.get("mentions_24h", 0)
            h72 = r.get("mentions_72h", 0)
            h168 = r.get("mentions_168h", 0)
            earliest = r.get("earliest_tweet") or "None"
        else:
            h1 = h6 = h12 = h24 = h72 = h168 = "X"
            earliest = "None"
        print(
            f"{r['ticker']:<12} {r['age_str']:<10} "
            f"{str(h1):<5} {str(h6):<5} {str(h12):<5} {str(h24):<5} "
            f"{str(h72):<5} {str(h168):<5}  {earliest}",
        )

    # Conclusion
    zero_in_1h = [
        r for r in results
        if r.get("mint") and r.get("mentions_1h", -1) == 0
    ]
    non_zero_in1h = [
        r for r in results
        if r.get("mint") and r.get("mentions_1h", -1) > 0
    ]

    print(f"\n{'=' * 60}")
    print(f"Zero 1h mentions: {len(zero_in_1h)} / {len(results)} resolved")
    if zero_in_1h:
        ages = [r.get("age_hours", 0) for r in zero_in_1h]
        first_zero = min(ages) if ages else 0
        print(f"Earliest coin with 0 mentions: ~{first_zero:.1f}h old")
    if non_zero_in1h:
        ages = [r.get("age_hours", 0) for r in non_zero_in1h]
        last_nonzero = max(ages)
        print(f"Newest coin with mentions: ~{last_nonzero:.1f}h old")
    print(f"{'=' * 60}")

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved %d results to %s", len(results), OUTPUT_PATH)


if __name__ == "__main__":
    asyncio.run(main())
