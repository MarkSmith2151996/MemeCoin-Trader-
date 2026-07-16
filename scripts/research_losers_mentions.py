"""Research: Grok X mention counts for failed PumpFun coins (noise baseline)."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import httpx

PUMPFUN_URL = "https://frontend-api-v3.pump.fun/coins"
GROK_API_URL = "https://api.x.ai/v1/responses"
OUTPUT_PATH = "data/losers_mentions.json"
log = logging.getLogger("research_losers")


def load_api_key() -> str | None:
    key = os.getenv("GROK_API_KEY")
    if key:
        return key
    env_path = ".env"
    if os.path.isfile(env_path):
        for line in open(env_path):
            stripped = line.strip()
            if stripped.startswith("GROK_API_KEY="):
                val = stripped.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val
    return None


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
        if len(failed) == 20:
            break
    return failed


def query_grok_mentions(api_key: str, symbol: str, mint: str, launch_date: str) -> int | None:
    prompt = (
        f"Search X for ${symbol} or {mint} posted on {launch_date}. "
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
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        log.warning("Grok API error for %s (%s): %s", symbol, mint[:8], exc)
        return None
    except ValueError as exc:
        log.warning("Grok API parse error for %s (%s): %s", symbol, mint[:8], exc)
        return None
    try:
        for output_item in data.get("output", []):
            content_blocks = output_item.get("content") if isinstance(output_item, dict) else None
            if isinstance(content_blocks, list):
                for item in content_blocks:
                    text = item.get("output_text") or item.get("text") or ""
                    match = re.search(r"\d+", text.strip())
                    if match:
                        return int(match.group())
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("Grok API unexpected response for %s (%s): %s", symbol, mint[:8], exc)
    return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    api_key = load_api_key()
    if not api_key:
        log.error("GROK_API_KEY not found in env or .env")
        return

    log.info("Fetching recent PumpFun launches...")
    coins = fetch_failed_coins()
    log.info("Found %d failed (non-graduated) coins", len(coins))

    if not coins:
        log.warning("No failed coins found")
        results: list[dict] = []
    else:
        results = []
        for i, coin in enumerate(coins, 1):
            symbol = coin.get("symbol", "?")
            mint = coin.get("mint", "")
            ts = coin.get("created_timestamp", 0)
            if isinstance(ts, (int, float)):
                launch_date = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            else:
                launch_date = "unknown"

            log.info("[%d/20] %s (%s) launched %s — querying Grok...", i, symbol, mint[:8], launch_date)
            mentions = query_grok_mentions(api_key, symbol, mint, launch_date)
            log.info("  -> day-1 mentions: %s", mentions)

            results.append({
                "ticker": symbol,
                "mint": mint,
                "launch_date": launch_date,
                "graduated": False,
                "day1_mentions": mentions,
            })

            if i < 20:
                time.sleep(2)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Saved %d results to %s", len(results), OUTPUT_PATH)

    sorted_results = sorted(results, key=lambda r: r["day1_mentions"] if r["day1_mentions"] is not None else -1, reverse=True)
    print("\n=== Losers Mentions Summary ===")
    print(f"{'Ticker':<12} {'Launch Date':<14} {'Graduated':<12} {'Day-1 Mentions':<16}")
    print("-" * 54)
    for r in sorted_results:
        m = str(r["day1_mentions"]) if r["day1_mentions"] is not None else "N/A"
        print(f"{r['ticker']:<12} {r['launch_date']:<14} {str(r['graduated']):<12} {m:<16}")

    winners_path = "data/winners_mentions.json"
    if os.path.isfile(winners_path):
        try:
            with open(winners_path) as f:
                winners = json.load(f)
            winner_mentions = [w["day1_mentions"] for w in winners if w.get("day1_mentions") is not None]
            loser_mentions = [r["day1_mentions"] for r in results if r["day1_mentions"] is not None]
            if winner_mentions and loser_mentions:
                avg_win = sum(winner_mentions) / len(winner_mentions)
                avg_lose = sum(loser_mentions) / len(loser_mentions)
                suggested = round((avg_lose * 2) / 5) * 5
                print("\n=== Comparison Summary ===")
                print(f"Average day-1 mentions (winners): {avg_win:.1f}")
                print(f"Average day-1 mentions (losers):  {avg_lose:.1f}")
                print(f"Suggested threshold (2x losers, round 5): {suggested}")
            else:
                print("\nNot enough mention data for comparison")
        except (json.JSONDecodeError, KeyError) as exc:
            log.warning("Could not read winners file: %s", exc)


if __name__ == "__main__":
    main()
