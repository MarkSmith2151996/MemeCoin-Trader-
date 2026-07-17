"""Influencer tracker runner — periodic scan of crypto influencer X accounts.

Scans every 5 minutes. Prints new mentions not seen in the previous cycle.
Logs to /tmp/influencer_tracker.log.

Usage:
    python3 scripts/run_influencer_tracker.py   # start scanning (manual stop)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.influencer_tracker import run_once

SCAN_INTERVAL_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/influencer_tracker.log"),
    ],
)
log = logging.getLogger("influencer_tracker_runner")


def _format_mcap(mcap) -> str:
    if mcap is None:
        return "N/A"
    try:
        val = float(mcap)
        if val >= 1_000_000:
            return f"${val / 1_000_000:.1f}M"
        elif val >= 1_000:
            return f"${val / 1_000:.1f}K"
        return f"${val:.0f}"
    except (TypeError, ValueError):
        return str(mcap)


def _format_vol(vol) -> str:
    if vol is None:
        return "N/A"
    try:
        val = float(vol)
        if val >= 1_000_000:
            return f"${val / 1_000_000:.1f}M"
        elif val >= 1_000:
            return f"${val / 1_000:.1f}K"
        return f"${val:.0f}"
    except (TypeError, ValueError):
        return str(vol)


def _format_age(age_h: float | None) -> str:
    if age_h is None:
        return "? old"
    if age_h < 1:
        return f"{int(age_h * 60)}min old"
    elif age_h < 24:
        return f"{age_h:.1f}h old"
    return f"{age_h / 24:.1f}d old"


async def main() -> None:
    seen_tickers: set[str] = set()
    log.info("Influencer tracker started (interval=%ds)", SCAN_INTERVAL_SECONDS)

    while True:
        now = datetime.now(UTC)
        timestamp = now.strftime("%H:%M UTC")
        log.info("=== Influencer Scan %s ===", timestamp)
        print(f"\n=== Influencer Scan {timestamp} ===")

        try:
            mentions = await run_once()
        except Exception as exc:
            log.error("run_once failed: %s", exc)
            print(f"  ERROR: {exc}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            continue

        # Separate new vs old mentions
        new_mentions = [m for m in mentions if m.get("ticker") and m["ticker"] not in seen_tickers]
        for m in mentions:
            if m.get("ticker"):
                seen_tickers.add(m["ticker"])

        # Print new mentions
        if new_mentions:
            for m in new_mentions:
                handle = m.get("handle", "?")
                ticker = m.get("ticker") or "?"
                mcap = _format_mcap(m.get("current_mcap"))
                age = _format_age(m.get("age_hours"))
                vol = _format_vol(m.get("volume_h1"))
                snippet = (m.get("snippet") or "")[:60]
                line = f"@{handle:<15} \u2192 ${ticker:<8} ({mcap} mcap, {age}, {vol} vol)"
                print(f"  {line}")
                log.info("NEW: %s", line)
        else:
            print("  No new tickers found")

        # Report accounts with no mentions
        handled = {m.get("handle") for m in mentions if m.get("handle")}
        from src.signals.influencer_tracker import INFLUENCER_ACCOUNTS

        silent = [a["handle"] for a in INFLUENCER_ACCOUNTS if a["handle"] not in handled]
        if silent:
            print(f"  No new mentions from: {', '.join('@' + h for h in silent)}")

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
