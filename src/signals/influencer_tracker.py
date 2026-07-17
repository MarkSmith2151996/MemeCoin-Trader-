"""Influencer tracker: monitor crypto influencer X accounts via Grok x_search.

Uses the Grok Responses API to search for recent posts from configured
influencer accounts, extracts $TICKER mentions and Solana mint addresses,
and enriches with live DexScreener market data.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

INFLUENCER_ACCOUNTS = [
    {"handle": "SolanaFloor", "type": "data"},
    {"handle": "lookonchain", "type": "data"},
    {"handle": "pumpdotfun", "type": "data"},
    {"handle": "blknoiz06", "type": "commentary"},
    {"handle": "0xVonGogh", "type": "commentary"},
    {"handle": "973Meech", "type": "commentary"},
]

GROK_API_URL = "https://api.x.ai/v1/responses"
DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEFAULT_MODEL = "grok-4.3"

TICKER_RE = re.compile(r"\$([A-Z]{2,10})(?:\b|\.)")
SOLANA_MINT_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")

log = logging.getLogger("influencer_tracker")


async def _grok_search(handle: str, hours: int = 1) -> str | None:
    """Search X for recent posts from @handle using Grok x_search tool.

    Returns the raw output text, or None on failure.
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        log.warning("GROK_API_KEY not set")
        return None

    prompt = (
        f"Search X for posts from @{handle} in the last {hours} hour(s). "
        "For each post, give me: the full post text, any $TICKER mentions, "
        "any Solana token mint addresses, and the approximate UTC timestamp "
        "in ISO format."
    )

    payload = {
        "model": DEFAULT_MODEL,
        "input": [{"role": "user", "content": prompt}],
        "tools": [{"type": "x_search"}],
    }

    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                resp = await client.post(
                    GROK_API_URL,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                )

            if resp.is_success:
                data = resp.json()
                text = _extract_output_text(data)
                if text:
                    return text
                return None

            status = resp.status_code
            if status == 429:
                log.warning("Grok 429 for @%s, attempt %d/3", handle, attempt)
                await asyncio.sleep(3)
                continue
            elif status in (500, 502, 503):
                log.warning("Grok %d for @%s, attempt %d/3", status, handle, attempt)
                await asyncio.sleep(2)
                continue
            else:
                log.warning("Grok HTTP %d for @%s, no retry", status, handle)
                return None

        except httpx.TimeoutException:
            log.warning("Grok timeout for @%s, attempt %d/3", handle, attempt)
            continue
        except Exception as exc:
            log.warning("Grok error for @%s: %s", handle, exc)
            return None

    log.warning("Grok failed for @%s after 3 attempts", handle)
    return None


def _extract_output_text(data: dict) -> str | None:
    try:
        content_blocks = data.get("output", [])
        for block in content_blocks:
            inner = block.get("content") if isinstance(block, dict) else None
            if isinstance(inner, list):
                for item in inner:
                    text = item.get("output_text") or item.get("text") or ""
                    if text.strip():
                        return text.strip()
    except (KeyError, TypeError, ValueError):
        pass
    return None


def _extract_mentions(
    text: str,
    handle: str,
) -> list[dict]:
    """Parse Grok response text into structured mention records.

    Extracts $TICKER mentions, Solana mint addresses, and timestamp hints.
    """
    records: list[dict] = []
    tickers = TICKER_RE.findall(text)
    mints = SOLANA_MINT_RE.findall(text)
    iso_ts_re = re.compile(
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?",
    )
    timestamps = iso_ts_re.findall(text)

    # Build mentions: one per unique ticker+mint pair found
    mentioned_pairs: set[tuple[str | None, str | None]] = set()
    for ticker in tickers:
        key = (ticker, None)
        if key not in mentioned_pairs:
            mentioned_pairs.add(key)
            records.append({
                "handle": handle,
                "ticker": ticker,
                "mint_hint": None,
                "posted_at": timestamps[0] if timestamps else None,
                "snippet": text[:100],
            })

    for mint in mints:
        key = (None, mint)
        if key not in mentioned_pairs:
            mentioned_pairs.add(key)
            records.append({
                "handle": handle,
                "ticker": None,
                "mint_hint": mint,
                "posted_at": timestamps[0] if timestamps else None,
                "snippet": text[:100],
            })

    return records


async def query_account(handle: str, hours: int = 1) -> list[dict]:
    """Query Grok for recent posts from a given X handle.

    Returns a list of mention records:
      [{handle, ticker, mint_hint, posted_at, snippet}, ...]
    """
    raw_text = await _grok_search(handle, hours)
    if not raw_text:
        return []
    return _extract_mentions(raw_text, handle)


async def enrich_with_dexscreener(mentions: list[dict]) -> list[dict]:
    """Enrich mention records with live DexScreener market data.

    For each mention with a ticker or mint_hint, calls DexScreener search
    and appends current_mcap, liquidity, volume_h1, age_hours, and chain.
    """
    enriched: list[dict] = []
    seen: set[str] = set()

    async with httpx.AsyncClient() as http:
        for m in mentions:
            ticker = m.get("ticker")
            mint_hint = m.get("mint_hint")
            query = mint_hint if mint_hint else (ticker if ticker else None)
            if not query or query in seen:
                enriched.append(m)
                continue

            seen.add(query)
            try:
                resp = await http.get(
                    DEXSCREENER_SEARCH,
                    params={"q": query},
                    timeout=10.0,
                )
                resp.raise_for_status()
                data = resp.json()
                pairs = data.get("pairs") or []
                # Pick the best Solana pair
                sol_pair = None
                for p in pairs:
                    if isinstance(p, dict) and p.get("chainId") == "solana":
                        sol_pair = p
                        break

                if sol_pair:
                    now = datetime.now(timezone.utc)
                    created_ms = sol_pair.get("pairCreatedAt")
                    age_h = None
                    if created_ms:
                        age_h = (now.timestamp() - created_ms / 1000) / 3600
                    m["current_mcap"] = sol_pair.get("marketCap") or sol_pair.get("fdv") or 0
                    m["liquidity"] = (sol_pair.get("liquidity") or {}).get("usd", 0)
                    m["volume_h1"] = (sol_pair.get("volume") or {}).get("h1", 0)
                    m["age_hours"] = round(age_h, 2) if age_h is not None else None
                    m["chain"] = "solana"
                else:
                    m["current_mcap"] = None
                    m["liquidity"] = None
                    m["volume_h1"] = None
                    m["age_hours"] = None
                    m["chain"] = None

            except Exception as exc:
                log.warning("DexScreener enrich failed for %s: %s", query, exc)
                m["current_mcap"] = None
                m["liquidity"] = None
                m["volume_h1"] = None
                m["age_hours"] = None
                m["chain"] = None

            enriched.append(m)

    return enriched


async def run_once() -> list[dict]:
    """Run one full scan cycle: query all 6 accounts, dedup, enrich, sort.

    Returns enriched mention records sorted by most recently posted.
    """
    all_mentions: list[dict] = []

    for account in INFLUENCER_ACCOUNTS:
        handle = account["handle"]
        log.info("Querying @%s ...", handle)
        try:
            records = await query_account(handle, hours=1)
            all_mentions.extend(records)
        except Exception as exc:
            log.warning("Failed to query @%s: %s", handle, exc)
        await asyncio.sleep(1.5)

    # Deduplicate by ticker
    seen_tickers: set[str] = set()
    deduped: list[dict] = []
    for m in all_mentions:
        ticker = m.get("ticker")
        if ticker and ticker not in seen_tickers:
            seen_tickers.add(ticker)
            deduped.append(m)
        elif not ticker:
            deduped.append(m)

    enriched = await enrich_with_dexscreener(deduped)

    # Sort by posted_at descending (most recent first)
    def _sort_key(m: dict) -> str:
        return m.get("posted_at") or ""

    enriched.sort(key=_sort_key, reverse=True)
    return enriched
