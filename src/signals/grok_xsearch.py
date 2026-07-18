"""Grok X search client for counting social mentions of a token."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

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
    """Extract a mention count from Grok response text.

    Prefers numbers near count/unique/total keywords; falls back to the last
    reasonable integer (ignoring mint-address-length digit sequences).
    """
    # Priority 1: keyword-adjacent numbers
    keyword_match = re.search(
        r"(?:count|total|unique)\s*[:=]?\s*(\d{1,9})\b",
        text, re.IGNORECASE,
    )
    if keyword_match:
        return int(keyword_match.group(1))

    # Priority 2: "X unique" or "X mentions"
    keyword_match2 = re.search(
        r"(\d{1,9})\s*(?:unique|mention|account|user|result)s?\b",
        text, re.IGNORECASE,
    )
    if keyword_match2:
        return int(keyword_match2.group(1))

    # Priority 3: last standalone integer that isn't absurdly long
    all_nums = re.findall(r"\b(\d{1,9})\b", text)
    if all_nums:
        return int(all_nums[-1])

    return None


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

    Delegates to get_mentions_with_timestamps and returns the 0-5min temporal
    bucket count. Returns 0 on any error.
    """
    launched_at = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    result = await get_mentions_with_timestamps(ticker, mint, launched_at, hours=1)
    return result.get("mentions_0_5min", 0)


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


INFLUENCER_HANDLES = [
    "SolanaFloor", "lookonchain", "pumpdotfun", "blknoiz06", "0xVonGogh", "973Meech",
]


async def count_influencer_mentions(
    ticker: str,
    mint: str,
    launched_at: datetime,
    window_minutes: int = 15,
) -> dict:
    """Query Grok for mentions ONLY from tracked influencer accounts within
    the first window_minutes after launch using since_time/until_time operators.

    Returns dict with: total (int), accounts_mentioned (list[str])
    """
    launch_unix = int(launched_at.timestamp())
    window_end = launch_unix + window_minutes * 60
    from_filters = " OR ".join(f"from:{h}" for h in INFLUENCER_HANDLES)

    for query_str in [f"${ticker}", mint]:
        q = f"({from_filters}) {query_str} since_time:{launch_unix} until_time:{window_end}"
        prompt = (
            f"Search X for: {q}\n"
            "List each mention with the account handle and approximate UTC timestamp (ISO format). "
            "Then report the total count of unique accounts from the specified list that mentioned it. "
            "Respond with the format: COUNT: <number>, ACCOUNTS: <handles or NONE>"
        )
        payload = {
            "model": DEFAULT_MODEL,
            "input": [{"role": "user", "content": prompt}],
            "tools": [{"type": "x_search"}],
        }
        data = await _post_with_retry(ticker, mint, payload)
        if data is None:
            continue

        text = _extract_output_text(data)
        if not text:
            continue

        raw_text = text
        if not any(h.lower() in raw_text.lower() for h in INFLUENCER_HANDLES):
            try:
                raw_json = json.dumps(data)
                if any(h.lower() in raw_json.lower() for h in INFLUENCER_HANDLES):
                    raw_text = raw_json
            except (TypeError, ValueError):
                pass

        # Extract handles by checking which appear on mention-like (timestamp-bearing) lines
        lines = raw_text.split("\n")
        accounts_mentioned: list[str] = []
        accounted: set[str] = set()
        for line in lines:
            l_lower = line.lower()
            for h in INFLUENCER_HANDLES:
                if h.lower() in l_lower and h not in accounted:
                    mention_keywords = ["mention", "post", "tweet", "said"]
                    if _ISO_TS_RE.search(line) or any(v in l_lower for v in mention_keywords):
                        accounted.add(h)
                        accounts_mentioned.append(h)

        # Extract count: prefer mention-result context, ignore "X accounts" listing
        mention_count = None
        mc = re.search(r'(\d{1,9})\s*(?:mention|result)s?\b', raw_text, re.IGNORECASE)
        if mc:
            mention_count = int(mc.group(1))
        else:
            mc2 = re.search(r'(?:count|total)\s*[:=]?\s*(\d{1,9})\b', raw_text, re.IGNORECASE)
            if mc2:
                mention_count = int(mc2.group(1))

        total = mention_count if mention_count is not None else len(accounts_mentioned)

        return {
            "total": total,
            "accounts_mentioned": accounts_mentioned,
        }

    return {"total": 0, "accounts_mentioned": []}


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
