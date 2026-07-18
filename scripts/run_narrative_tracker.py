"""Narrative tracker runner — periodic scan of high-signal accounts.

Scans every 5 minutes. Prints trending keywords, account posts, and matching
fresh Solana coins. Only prints new matches not seen in the previous cycle.
Logs to /tmp/narrative_tracker.log.

Usage:
    python3 scripts/run_narrative_tracker.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.signals.narrative_tracker import run_once

SCAN_INTERVAL_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/narrative_tracker.log"),
    ],
)
log = logging.getLogger("narrative_tracker_runner")


def _fmt_usd(val) -> str:
    if val is None:
        return "N/A"
    try:
        v = float(val)
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        elif v >= 1_000:
            return f"${v / 1_000:.1f}K"
        return f"${v:.0f}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_age(minutes: float) -> str:
    if minutes < 1:
        return f"{int(minutes * 60)}s"
    return f"{int(minutes)}min"


async def main() -> None:
    seen_mints: set[str] = set()
    log.info("Narrative tracker started (interval=%ds)", SCAN_INTERVAL_SECONDS)

    while True:
        now = datetime.now(UTC)
        timestamp = now.strftime("%H:%M UTC")
        log.info("=== Narrative Scan %s ===", timestamp)
        print(f"\n=== Narrative Scan {timestamp} ===")

        try:
            report = await run_once()
        except Exception as exc:
            log.error("run_once failed: %s", exc)
            print(f"  ERROR: {exc}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            continue

        keywords = report.get("keywords", [])
        keyword_accounts = report.get("keyword_accounts", {})
        total_posts = report.get("total_posts", 0)
        total_accounts = report.get("total_accounts_with_posts", 0)
        account_posts = report.get("account_posts", [])
        matches = report.get("matches", [])
        no_match_keywords = report.get("no_match_keywords", [])

        # Trending keywords
        if keywords:
            kw_line = ", ".join(keywords[:10])
            print(f"\nTrending keywords: {kw_line}")
            print(f"(from {total_posts} posts across {total_accounts} accounts)")
        else:
            print("\nNo keywords extracted")

        # Account posts
        if account_posts:
            shown_handles: set[str] = set()
            for post in account_posts:
                handle = post.get("handle", "?")
                if handle in shown_handles:
                    continue
                shown_handles.add(handle)
                text = (post.get("post_text") or "")[:80].replace("\n", " ")
                ago = ""
                ts = post.get("posted_at")
                if ts:
                    try:
                        posted = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        delta = (datetime.now(UTC) - posted).total_seconds()
                        if delta < 120:
                            ago = " (just now)"
                        elif delta < 3600:
                            ago = f" ({int(delta // 60)} min ago)"
                        else:
                            ago = f" ({delta / 3600:.1f}h ago)"
                    except (ValueError, TypeError):
                        pass
                print(f"@{handle:<20} \u2192 {text}{ago}")
            print()

        # New matches
        new_matches = [m for m in matches if m.get("mint") not in seen_mints]
        for m in matches:
            if m.get("mint"):
                seen_mints.add(m["mint"])

        if new_matches:
            print("Matching fresh coins:")
            for m in new_matches:
                ticker = m.get("ticker", "?")
                mint = (m.get("mint") or "?")[:8]
                age = _fmt_age(m.get("age_min", 0))
                mcap = _fmt_usd(m.get("mcap"))
                liq = _fmt_usd(m.get("liquidity"))
                vol = _fmt_usd(m.get("volume_h1"))
                kw = m.get("keyword", "?")
                print(
                    f"  {ticker:<8} (mint: {mint}...) "
                    f"age={age}  mcap={mcap}  liq={liq}  vol={vol} "
                    f"[match: {kw}]"
                )
            print()
        else:
            print("No new matching coins found\n")

        # Keywords with no matches
        if no_match_keywords:
            print(f"No matches for: {', '.join(no_match_keywords[:8])}")

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
