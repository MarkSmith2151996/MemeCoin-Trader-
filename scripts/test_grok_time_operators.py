"""Test whether Grok x_search respects since_time/until_time operators.

Usage:
    python3 scripts/test_grok_time_operators.py

Saves raw output to research/mt479_output/grok_time_operator_test.json.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.grok_xsearch import _post_with_retry, _extract_output_text, _parse_iso_timestamps

load_dotenv()

OUTPUT_PATH = "research/mt479_output/grok_time_operator_test.json"

COINS = [
    {
        "ticker": "WOOD",
        "mint": "aNM35husY4h11AgD6F5Wqd9BgoaWMyJQBBSGBndpump",
        "launched_at": "2026-07-12 17:04:19",
    },
    {
        "ticker": "SKHY",
        "mint": "Co8gzijzHJdNEWoCayMX43oeoe5WiMBje9Vt7knD7Mo6",
        "launched_at": "2026-07-11 03:14:12",
    },
    {
        "ticker": "HOODIE",
        "mint": "4euHEW3XCxJtj4jyRxGboQjv9d5T6Bg9q2pA7KM3ZaAC",
        "launched_at": "2026-07-08 20:29:07",
    },
    {
        "ticker": "INDEX",
        "mint": "XqF7TY2r3UbuTya71stLcoejHiHGKy38UJUu8itpump",
        "launched_at": "2026-07-15 20:07:10",
    },
    {
        "ticker": "HOOD",
        "mint": "Bf4DZ8rzMiuyLnVdUCwP7bV8L4UW7x7mAAasuqrVhQUJ",
        "launched_at": "2026-07-13 22:50:26",
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("grok_time_ops")


def to_unix(dt_str: str) -> int:
    return int(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


async def query_grok(ticker: str, mint: str, query: str) -> dict | None:
    """Send a Grok x_search query and return the full response dict."""
    prompt = (
        f"Search X for: {query}\n"
        "List each mention with its approximate UTC timestamp (ISO format). "
        "Then report the total count of unique accounts that mentioned it."
    )
    payload = {
        "model": "grok-4.3",
        "input": [{"role": "user", "content": prompt}],
        "tools": [{"type": "x_search"}],
    }
    return await _post_with_retry(ticker, mint, payload)


async def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    raw_responses: dict = {}
    results: list[dict] = []

    for coin in COINS:
        ticker = coin["ticker"]
        mint = coin["mint"]
        launch_unix = to_unix(coin["launched_at"])
        window_end_unix = launch_unix + 300

        print(f"\n{'=' * 70}")
        print(f"{ticker:<6} mint: {mint}")
        print(f"{'':6} launch_unix: {launch_unix}  window_end_unix: {window_end_unix}")

        coin_result = {
            "ticker": ticker,
            "mint": mint,
            "launched_at": coin["launched_at"],
            "launch_unix": launch_unix,
            "window_end_unix": window_end_unix,
            "ticker_query": None,
            "mint_query": None,
        }

        for label, q in [("ticker", f"${ticker}"), ("mint", mint)]:
            query = f'{q} since_time:{launch_unix} until_time:{window_end_unix}'
            print(f"\n  --- Query ({label}): {query}")
            coin_result[f"{label}_query_sent"] = query

            data = await query_grok(ticker, mint, query)
            raw_responses[f"{ticker}_{label}"] = data
            coin_result[f"{label}_raw"] = data

            if data is None:
                print(f"  Error: No response (API error or auth)")
                coin_result[f"{label}_error"] = "no_response"
                coin_result[f"{label}_mentions"] = 0
                coin_result[f"{label}_timestamps"] = []
                coin_result[f"{label}_in_window"] = 0
                continue

            text = _extract_output_text(data)
            print(f"  Raw text length: {len(text) if text else 0} chars")

            if text:
                timestamps = _parse_iso_timestamps(text)
            else:
                # Fallback: search raw JSON
                raw_json = json.dumps(data)
                timestamps = _parse_iso_timestamps(raw_json)

            in_window = sum(
                1 for ts in timestamps
                if launch_unix <= ts.timestamp() <= window_end_unix
            )

            print(f"  mentions returned (ISO ts found): {len(timestamps)}")
            for ts in timestamps:
                ts_unix = int(ts.timestamp())
                marker = "✓" if launch_unix <= ts_unix <= window_end_unix else "✗"
                print(f"    {ts.isoformat()}  unix={ts_unix}  {marker}")

            coin_result[f"{label}_mentions"] = len(timestamps)
            coin_result[f"{label}_timestamps"] = [ts.isoformat() for ts in timestamps]
            coin_result[f"{label}_in_window"] = in_window

            await asyncio.sleep(2)

        # Determine verdict for this coin
        ticker_ok = coin_result["ticker_mentions"] > 0
        mint_ok = coin_result["mint_mentions"] > 0

        if ticker_ok or mint_ok:
            combined_in = coin_result["ticker_in_window"] + coin_result["mint_in_window"]
            combined_total = coin_result["ticker_mentions"] + coin_result["mint_mentions"]
            if combined_total > 0:
                pct = combined_in / combined_total
                verdict = "OPERATORS WORKING ✓" if pct >= 0.5 else "OPERATORS NOT RESPECTED ✗"
            else:
                verdict = "NO MENTIONS FOUND -"
        else:
            verdict = "NO MENTIONS FOUND -"

        coin_result["verdict"] = verdict
        results.append(coin_result)

        print(f"\n  VERDICT: {verdict}")

    # Summary
    print(f"\n{'=' * 70}")
    print(f"{'TICKER':<8} {'QUERY':<50} {'MENTIONS':<10} {'IN-WINDOW':<12} {'VERDICT'}")
    print("-" * 100)

    working_count = 0
    for r in results:
        t = r["ticker"]
        for label in ["ticker", "mint"]:
            q = r.get(f"{label}_query_sent", "")
            mentions = r.get(f"{label}_mentions", 0)
            in_win = r.get(f"{label}_in_window", 0)
            v = r["verdict"]
            if mentions > 0 and mentions == in_win:
                v_short = "OP OK ✓"
            elif mentions > 0 and in_win < mentions:
                v_short = "PARTIAL"
            else:
                v_short = "NONE"
            print(f"{t:<8} {q:<50} {str(mentions):<10} {str(in_win):<12} {v_short}")
        print()

    working = sum(1 for r in results if "WORKING" in r["verdict"])
    print(f"\nOperators working on {working}/{len(results)} coins")
    if working >= 3:
        print("→ Can proceed with full 20-coin historical research")
    else:
        print("→ Operators unreliable — need alternative approach")
    print(f"{'=' * 70}")

    with open(OUTPUT_PATH, "w") as f:
        json.dump({"results": results, "raw_responses": raw_responses}, f, indent=2, default=str)
    log.info("Saved output to %s", OUTPUT_PATH)


if __name__ == "__main__":
    asyncio.run(main())
