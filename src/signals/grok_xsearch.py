"""Grok X search client for counting social mentions of a token."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

import httpx

GROK_API_URL = "https://api.x.ai/v1/responses"
DEFAULT_MODEL = "grok-4.3"

log = logging.getLogger("grok_xsearch")

_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)


async def _post_with_retry(
    ticker: str,
    mint: str,
    payload: dict,
    max_attempts: int = 3,
) -> dict | None:
    """POST to Grok API with retry logic. Returns parsed JSON or None."""
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        return None

    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
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
                return resp.json()

            status = resp.status_code
            if status == 429:
                if attempt < max_attempts:
                    log.warning(
                        "Grok 429 for %s (%s), attempt %d/%d, waiting 3s",
                        ticker, mint[:8], attempt, max_attempts,
                    )
                    await asyncio.sleep(3)
                continue
            elif status in (500, 502, 503):
                if attempt < max_attempts:
                    log.warning(
                        "Grok %d for %s (%s), attempt %d/%d, waiting 2s",
                        status, ticker, mint[:8], attempt, max_attempts,
                    )
                    await asyncio.sleep(2)
                continue
            else:
                log.warning(
                    "Grok API HTTP %d for %s (%s), no retry",
                    status, ticker, mint[:8],
                )
                return None

        except httpx.TimeoutException:
            if attempt < max_attempts:
                log.warning(
                    "Grok timeout for %s (%s), attempt %d/%d, retrying immediately",
                    ticker, mint[:8], attempt, max_attempts,
                )
                continue
            last_exc = httpx.TimeoutException("Grok API timeout")
        except httpx.HTTPError as exc:
            log.warning(
                "Grok API error for %s (%s): %s, no retry",
                ticker, mint[:8], exc,
            )
            return None
        except ValueError as exc:
            log.warning(
                "Grok API parse error for %s (%s): %s, no retry",
                ticker, mint[:8], exc,
            )
            return None

    if last_exc:
        log.warning("Grok API failed for %s (%s) after %d attempts", ticker, mint[:8], max_attempts)
    return None


def _extract_output_text(data: dict) -> str | None:
    """Extract output_text from the Grok response content blocks."""
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


def _extract_number(text: str) -> int | None:
    match = re.search(r"\d+", text.strip())
    return int(match.group()) if match else None


def _parse_iso_timestamps(text: str) -> list[datetime]:
    """Extract all ISO-format UTC timestamps from a string."""
    results = []
    for match in _ISO_TS_RE.finditer(text):
        raw = match.group()
        raw = raw.replace(" ", "T")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        elif "+" in raw[10:] or "-" in raw[10:]:
            sep = raw[10:].find("-")
            if sep >= 0:
                tz_part = raw[10 + sep:]
                if ":" not in tz_part and len(tz_part) == 5:
                    tz_part = tz_part[:3] + ":" + tz_part[3:]
                raw = raw[:10 + sep] + tz_part
        else:
            raw += "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            results.append(dt)
        except (ValueError, TypeError):
            continue
    return results


def _bucket_mentions(
    timestamps: list[datetime],
    launched_at: datetime,
    total_mentions: int,
) -> dict:
    """Bucket mention timestamps into time windows relative to launch."""
    if not timestamps:
        return {
            "total_mentions": total_mentions,
            "mentions_0_5min": 0,
            "mentions_5_15min": 0,
            "mentions_15_60min": 0,
            "mentions_after_60min": 0,
            "earliest_mention_min": None,
            "mention_timestamps": [],
        }

    minutes_list = []
    ts_strings = []
    for ts in timestamps:
        delta = (ts - launched_at).total_seconds() / 60.0
        minutes_list.append(delta)
        ts_strings.append(ts.isoformat())

    earliest_min = min(minutes_list) if minutes_list else None

    buckets = {"0_5min": 0, "5_15min": 0, "15_60min": 0, "after_60min": 0}
    for m in minutes_list:
        if m < 0:
            continue
        if m <= 5:
            buckets["0_5min"] += 1
        elif m <= 15:
            buckets["5_15min"] += 1
        elif m <= 60:
            buckets["15_60min"] += 1
        else:
            buckets["after_60min"] += 1

    return {
        "total_mentions": total_mentions,
        "mentions_0_5min": buckets["0_5min"],
        "mentions_5_15min": buckets["5_15min"],
        "mentions_15_60min": buckets["15_60min"],
        "mentions_after_60min": buckets["after_60min"],
        "earliest_mention_min": earliest_min,
        "mention_timestamps": ts_strings[:20],
    }


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

    data = await _post_with_retry(ticker, mint, payload)
    if data is None:
        return 0

    text = _extract_output_text(data)
    if text:
        val = _extract_number(text)
        if val is not None:
            return val

    try:
        content_blocks = data.get("output", [])
        for block in content_blocks:
            inner = block.get("content") if isinstance(block, dict) else None
            if isinstance(inner, list):
                for item in inner:
                    text2 = item.get("output_text") or item.get("text") or ""
                    match = re.search(r"\d+", text2.strip())
                    if match:
                        return int(match.group())
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("Grok API unexpected response shape for %s (%s): %s", ticker, mint[:8], exc)

    return 0


async def get_mentions_with_timestamps(
    ticker: str,
    mint: str,
    launched_at: datetime,
    hours: int = 24,
) -> dict:
    """Query Grok x_search for mentions of $ticker or mint in the last `hours` hours.

    Returns a dict with:
      - total_mentions: int
      - mentions_0_5min: int   (mentions within 5 min of launched_at)
      - mentions_5_15min: int  (5-15 min post-launch)
      - mentions_15_60min: int (15-60 min post-launch)
      - mentions_after_60min: int (>60 min post-launch)
      - earliest_mention_min: float | None
      - mention_timestamps: list[str]
    Returns all zeros on any error.
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        log.warning("GROK_API_KEY not set")
        return _zero_mention_result()

    prompt = (
        f"Search X for ${ticker} or {mint} in the last {hours} hours. "
        "List each mention with its approximate UTC timestamp (ISO format). "
        "Then report the total count of unique accounts that mentioned it."
    )

    payload = {
        "model": DEFAULT_MODEL,
        "input": [{"role": "user", "content": prompt}],
        "tools": [{"type": "x_search"}],
    }

    data = await _post_with_retry(ticker, mint, payload)
    if data is None:
        return _zero_mention_result()

    text = _extract_output_text(data)
    timestamps: list[datetime] = []
    total_mentions = 0

    if text:
        timestamps = _parse_iso_timestamps(text)
        raw_count = _extract_number(text)
        if raw_count is not None:
            total_mentions = raw_count

    # Regex fallback over the raw response JSON string
    if not timestamps and not total_mentions:
        try:
            raw_json = json.dumps(data)
            fallback_tss = _parse_iso_timestamps(raw_json)
            fallback_num = _extract_number(raw_json)
            if fallback_tss or fallback_num:
                log.warning(
                    "Regex fallback used for %s (%s) — structured parse found no timestamps",
                    ticker, mint[:8],
                )
                timestamps = fallback_tss
                if fallback_num is not None:
                    total_mentions = fallback_num
        except (TypeError, ValueError):
            pass

    if not timestamps and total_mentions:
        log.warning(
            "Fallback to total-only for %s (%s) — count=%d, no parseable timestamps",
            ticker, mint[:8], total_mentions,
        )
        return {
            "total_mentions": total_mentions,
            "mentions_0_5min": None,
            "mentions_5_15min": None,
            "mentions_15_60min": None,
            "mentions_after_60min": None,
            "earliest_mention_min": None,
            "mention_timestamps": [],
        }

    return _bucket_mentions(timestamps, launched_at, total_mentions)


def _zero_mention_result() -> dict:
    return {
        "total_mentions": 0,
        "mentions_0_5min": 0,
        "mentions_5_15min": 0,
        "mentions_15_60min": 0,
        "mentions_after_60min": 0,
        "earliest_mention_min": None,
        "mention_timestamps": [],
    }
