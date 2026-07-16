"""Grok X search client for counting social mentions of a token."""

from __future__ import annotations

import logging
import os
import re

import httpx

GROK_API_URL = "https://api.x.ai/v1/responses"
DEFAULT_MODEL = "grok-4.3"

log = logging.getLogger("grok_xsearch")


async def count_unique_mentions(ticker: str, mint: str, minutes: int = 5) -> int:
    """Query Grok's x_search tool for unique X accounts mentioning $ticker or mint.

    Returns the count as an integer, or 0 on any error.
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        log.warning("GROK_API_KEY not set")
        return 0

    prompt = (
        f"Search X for ${ticker} or {mint} posted in the last {minutes} minutes. "
        "Count unique accounts that mentioned it. Reply with just the integer."
    )

    payload = {
        "model": DEFAULT_MODEL,
        "input": prompt,
        "tools": [{"type": "x_search"}],
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                GROK_API_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        log.warning("Grok API timeout for %s (%s)", ticker, mint[:8])
        return 0
    except httpx.HTTPStatusError as exc:
        log.warning("Grok API HTTP %d for %s (%s)", exc.response.status_code, ticker, mint[:8])
        return 0
    except httpx.HTTPError as exc:
        log.warning("Grok API error for %s (%s): %s", ticker, mint[:8], exc)
        return 0
    except ValueError as exc:
        log.warning("Grok API parse error for %s (%s): %s", ticker, mint[:8], exc)
        return 0

    try:
        content_blocks = data.get("output", [])
        for block in content_blocks:
            inner = block.get("content") if isinstance(block, dict) else None
            if isinstance(inner, list):
                for item in inner:
                    text = item.get("output_text") or item.get("text") or ""
                    match = re.search(r"\d+", text.strip())
                    if match:
                        return int(match.group())
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("Grok API unexpected response shape for %s (%s): %s", ticker, mint[:8], exc)

    return 0
