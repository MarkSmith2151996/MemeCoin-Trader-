"""Standalone whale tracker monitoring loop.

Usage:
    python scripts/run_whale_tracker.py

Runs every 60 seconds:
  1. Load 50 tracked wallets from config/tracked_wallets.json
  2. Check recent buys across all wallets
  3. Print fresh coin signals
  4. Log to /tmp/whale_tracker.log
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.whale_tracker import (
    check_fresh_coin_buys,
    enrich_wallet_pnl,
    load_tracked_wallets,
)

LOG_PATH = Path("/tmp/whale_tracker.log")
SCAN_INTERVAL_S = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH)),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("whale_tracker")


def _fmt_age(minutes: float) -> str:
    if minutes < 1:
        return f"{int(minutes * 60)}s"
    return f"{int(minutes)}min"


async def scan_cycle(http: httpx.AsyncClient, cycle: int) -> None:
    """Run one whale scan cycle."""
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%H:%M UTC")

    wallets = load_tracked_wallets()
    if not wallets:
        log.warning("No tracked wallets loaded")
        return

    log.info("=== Whale Scan %d | %s | %d wallets ===", cycle, timestamp, len(wallets))

    signals = await check_fresh_coin_buys(wallets, http, max_age_minutes=15)

    if signals:
        for sig in signals:
            age_str = _fmt_age(sig["age_min"])
            scores = sig.get("whale_scores", [])
            mult = 1.0
            count = sig["whale_count"]
            if count == 1:
                mult = 2.0
            elif count == 2:
                mult = 4.0
            elif count >= 3:
                mult = 6.0

            lines = [
                f"  SIGNAL: {sig['ticker']} ({sig['mint'][:8]}...)",
                f"    Age: {age_str}  MCap: ${sig['mcap']:,}  Liq: ${sig['liquidity']:,.0f}",
                f"    Whales in: {count} (scores: {', '.join(str(s) for s in scores)})",
                f"    Size multiplier: {mult}x",
            ]
            for line in lines:
                log.info(line)

        # Enrich one top signal wallet for display
        top_sig = signals[0]
        top_wallet = top_sig["whale_addresses"][0]
        pnl = await enrich_wallet_pnl(top_wallet, http)
        if pnl.get("data_source") and pnl["data_source"] != "none":
            log.info(
                "  Top whale PnL: %s SOL (source: %s)",
                pnl.get("estimated_pnl_sol") or "?",
                pnl["data_source"],
            )
    else:
        log.info("No whale activity on fresh coins this cycle.")


async def main() -> None:
    log.info("Whale tracker started — scanning every %ds", SCAN_INTERVAL_S)
    log.info("Logging to %s", LOG_PATH)

    cycle = 0
    async with httpx.AsyncClient(timeout=15.0) as http:
        while True:
            try:
                cycle += 1
                await scan_cycle(http, cycle)
            except Exception as exc:
                log.error("Scan cycle %d failed: %s", cycle, exc)

            await asyncio.sleep(SCAN_INTERVAL_S)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Whale tracker stopped by user.")
        sys.exit(0)
