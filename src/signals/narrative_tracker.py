"""Narrative tracker: monitor high-signal Twitter accounts, extract narratives,
find matching fresh Solana coins on DexScreener."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

GROK_API_URL = "https://api.x.ai/v1/responses"
DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEFAULT_MODEL = "grok-4.3"
ACCOUNTS_PATH = Path(__file__).parent.parent.parent / "config" / "narrative_accounts.json"

log = logging.getLogger("narrative_tracker")

STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "this", "that", "these",
    "those", "it", "its", "they", "them", "we", "you", "he", "she",
    "not", "no", "nor", "so", "if", "then", "than", "too", "very",
    "just", "about", "up", "out", "off", "over", "also", "into",
    "more", "some", "any", "each", "every", "all", "both", "few",
    "most", "other", "such", "only", "own", "same", "while", "because",
    "after", "before", "between", "under", "again", "further", "once",
    "here", "there", "when", "where", "why", "how", "what", "which",
    "who", "whom", "whose", "get", "got", "make", "made", "take",
    "know", "see", "say", "said", "think", "come", "go", "give",
    "find", "tell", "let", "like", "now", "really", "way", "back",
    "still", "even", "much", "well", "first", "last", "long", "great",
    "big", "little", "high", "right", "good", "thanks", "please", "yes",
    "no", "ok", "hey", "oh", "ah", "im", "dont", "wont", "cant",
    "didnt", "wasnt", "couldnt", "wouldnt", "shouldnt", "hasnt",
    "isnt", "arent", "ive", "youve", "hes", "shes", "its", "weve",
    "theyve", "id", "youd", "hed", "shed", "wed", "theyd", "ill",
    "youll", "hell", "shell", "well", "theyll", "whats", "heres",
    "theres", "wheres", "whos", "whys", "hows", "lets", "thats",
    "doesnt", "amp", "via", "https", "http", "com", "org", "net",
    "rt", "retweet", "just", "today", "tomorrow", "yesterday",
    "going", "wants", "needs", "look", "looks", "looking",
    "seems", "feels", "feeling", "thinks", "thinking", "knows",
    "utm", "source", "campaign", "medium", "www", "html",
    "index", "ref", "link", "links", "click", "track",
    "t01", "t00", "t02", "t03", "t04", "t05",
    "get", "got", "got", "see", "come", "go", "let", "take",
    "january", "february", "march", "april", "june", "july",
    "august", "september", "october", "november", "december",
    "monday", "tuesday", "wednesday", "thursday", "friday",
    "saturday", "sunday", "jan", "feb", "mar", "apr", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec", "mon", "tue",
    "wed", "thu", "fri", "sat", "sun",
    "year", "day", "week", "month", "hour", "minute", "second",
    "today", "yesterday", "tomorrow", "ago", "now", "later",
    "thing", "things", "something", "everything", "nothing",
    "people", "person", "guy", "dude", "man", "woman", "girl",
    "boy", "child", "kid", "life", "world", "time", "way",
    "someone", "anyone", "everyone", "saying", "talking", "doing",
    "going", "getting", "making", "taking", "looking", "trying",
    "actually", "basically", "literally", "essentially",
    "mean", "means", "meaning", "guess", "suppose",
    "pretty", "quite", "rather", "almost", "nearly",
    "maybe", "perhaps", "probably", "definitely", "certainly",
    "as", "her", "one", "my", "his", "our", "their", "its",
    "mine", "yours", "hers", "theirs", "ours",
    "posts", "post", "utc", "gmt", "found", "here", "note",
    "response", "results", "result", "list", "listing",
}

MEMEABLE_TERMS = {
    "dog", "cat", "frog", "pepe", "moon", "mars", "rocket", "diamond",
    "hands", "chad", "based", "wagmi", "anon", "fren", "whale", "shark",
    "bull", "bear", "wolf", "fox", "ape", "monkey", "bird", "fish",
    "dragon", "lion", "tiger", "panda", "koala", "sloth", "otter",
    "duck", "chicken", "horse", "cow", "pig", "sheep", "goat", "rat",
    "mouse", "rabbit", "turtle", "snake", "eagle", "hawk", "shib",
    "inu", "floki", "samoyed", "husky", "corgi", "doge", "bonk",
    "woof", "ai", "agi", "gpt", "llm", "neural", "agent", "bot",
    "robot", "quantum", "blockchain", "defi", "nft", "metaverse",
    "dao", "layer2", "bridge", "oracle", "privacy", "zk", "rollup",
    "trump", "biden", "election", "vote", "maga", "patriot", "liberty",
    "freedom", "economy", "inflation", "recession", "gold", "silver",
    "oil", "gas", "energy", "solar", "musk", "bezos", "buffett",
    "wojak", "chill", "gigachad", "mfer", "degen", "punk", "cyber",
    "matrix", "terminus", "mars", "starship", "marijuana", "weed",
    "crypto", "bitcoin", "ethereum", "solana", "bnb", "meme", "memecoin",
    "rug", "pump", "dump", "hodl", "fomo", "fud", "burn", "stake",
    "yield", "farm", "harvest", "airdrop", "launch", "presale",
}

_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)
_URL_RE = re.compile(r"https?://\S+")


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


def _parse_posts(text: str, handle: str) -> list[dict]:
    records: list[dict] = []
    timestamps = _ISO_TS_RE.findall(text)
    handle_lower = handle.lower()
    for line in text.split("\n"):
        line = line.strip()
        if not line or len(line) < 10:
            continue
        if "no posts" in line.lower():
            continue
        if f"@{handle_lower}" not in line.lower():
            continue
        ts = None
        for t in timestamps:
            if t in line:
                ts = t
                break
        text_part = line
        for t in timestamps:
            text_part = text_part.replace(t, "").replace("|", "").strip()
        text_part = re.sub(rf"@\w+\s*\|?\s*", "", text_part).strip()
        text_part = re.sub(r"\s{2,}", " ", text_part).strip()
        if len(text_part) < 2:
            continue
        records.append({
            "handle": handle,
            "post_text": text_part[:500],
            "posted_at": ts or (timestamps[0] if timestamps else None),
        })
    if not records and text and f"@{handle_lower}" in text.lower():
        records.append({
            "handle": handle,
            "post_text": text[:500],
            "posted_at": timestamps[0] if timestamps else None,
        })
    return records


async def _query_single_account(handle: str, hours: int = 1) -> list[dict]:
    """Query Grok for recent posts from a single account."""
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        return []

    prompt = (
        f"X posts last {hours}h from: {handle}. "
        "For each: @handle | post text | UTC time. "
        "Separate with blank lines."
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
                text = _extract_output_text(resp.json())
                if text:
                    return _parse_posts(text, handle)
                return []
            if resp.status_code == 429:
                if attempt < 3:
                    await asyncio.sleep(3)
                continue
            elif resp.status_code in (500, 502, 503):
                if attempt < 3:
                    await asyncio.sleep(2)
                continue
            else:
                return []
        except httpx.TimeoutException:
            log.warning("httpx timeout for @%s attempt %d/3", handle, attempt)
            if attempt < 3:
                continue
            return []
        except Exception as exc:
            log.warning("httpx error for @%s: %s", handle, exc)
            return []
    return []


def extract_narrative_keywords(posts: list[dict]) -> list[str]:
    """Extract meme-able keywords from post texts.

    Strips stopwords, scores by frequency and cross-account mention count,
    boosts known memeable terms. Returns deduplicated list sorted by score.
    """
    word_counts: Counter[str] = Counter()
    account_sets: dict[str, set[str]] = {}

    for post in posts:
        text = post.get("post_text", "")
        text = _URL_RE.sub("", text)
        handle = post.get("handle", "")
        words = re.findall(r"[A-Za-z][A-Za-z0-9]{1,24}", text)
        seen_in_post: set[str] = set()

        for word in words:
            lower = word.lower()
            if lower in STOPWORDS or len(lower) < 2:
                continue
            if lower in seen_in_post:
                continue
            seen_in_post.add(lower)

            word_counts[lower] += 1
            if lower not in account_sets:
                account_sets[lower] = set()
            account_sets[lower].add(handle)

    # Load tracked handle suffixes to filter them out
    tracked_handles_lower = {h.lower() for h in _load_accounts()}

    scored: list[tuple[float, str]] = []
    for word, count in word_counts.items():
        if word in tracked_handles_lower:
            continue
        cross = len(account_sets.get(word, set()))
        meme_boost = 3.0 if word in MEMEABLE_TERMS else 1.0
        if 2 <= cross <= 6:
            narrative_boost = 2.0
        elif cross == 1:
            narrative_boost = 1.0
        else:
            narrative_boost = 0.3
        score = count * meme_boost * narrative_boost
        scored.append((score, word))

    scored.sort(reverse=True)
    return [word for _, word in scored]


async def find_matching_coins(
    keywords: list[str],
    max_age_minutes: int = 30,
) -> list[dict]:
    """Search DexScreener for fresh Solana coins matching narrative keywords.

    Filters to Solana only, age < max_age_minutes, liquidity > $1K.
    Deduplicates by mint. Returns sorted by age (freshest first).
    """
    matches: list[dict] = []
    seen_mints: set[str] = set()
    now = datetime.now(timezone.utc)

    async with httpx.AsyncClient() as http:
        for keyword in keywords[:15]:
            try:
                resp = await http.get(
                    DEXSCREENER_SEARCH,
                    params={"q": keyword},
                    timeout=10.0,
                )
                resp.raise_for_status()
                data = resp.json()
                pairs = data.get("pairs") or []

                for p in pairs:
                    if not isinstance(p, dict) or p.get("chainId") != "solana":
                        continue

                    mint = p.get("baseToken", {}).get("address", "")
                    if not mint or mint in seen_mints:
                        continue

                    created_ms = p.get("pairCreatedAt")
                    if not created_ms:
                        continue
                    age_min = (now.timestamp() - created_ms / 1000) / 60
                    if age_min > max_age_minutes:
                        continue

                    liquidity_usd = (p.get("liquidity") or {}).get("usd", 0)
                    if liquidity_usd is None or float(liquidity_usd) < 1000:
                        continue

                    seen_mints.add(mint)
                    matches.append({
                        "keyword": keyword,
                        "ticker": p.get("baseToken", {}).get("symbol", "?"),
                        "mint": mint,
                        "launched_at": datetime.fromtimestamp(
                            created_ms / 1000, tz=timezone.utc
                        ).isoformat(),
                        "mcap": p.get("marketCap") or p.get("fdv") or 0,
                        "liquidity": float(liquidity_usd),
                        "volume_h1": (p.get("volume") or {}).get("h1", 0) or 0,
                        "age_min": round(age_min, 1),
                    })

            except Exception as exc:
                log.warning("DexScreener search failed for '%s': %s", keyword, exc)

            await asyncio.sleep(0.3)

    matches.sort(key=lambda m: m["age_min"])
    return matches


def _load_accounts() -> list[str]:
    try:
        with open(ACCOUNTS_PATH) as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        log.warning("Failed to load accounts: %s", exc)
        return []


async def run_once() -> dict[str, Any]:
    """Run one full narrative scan cycle.

    1. Load accounts from config/narrative_accounts.json
    2. Query Grok in batches of 20 → get recent posts
    3. Extract narrative keywords
    4. Find matching fresh coins on DexScreener

    Returns dict with keywords, account_posts, matches, no_match_keywords.
    """
    accounts = _load_accounts()
    if not accounts:
        log.warning("No accounts loaded from %s", ACCOUNTS_PATH)
        return {"keywords": [], "account_posts": [], "matches": [], "no_match_keywords": []}

    api_key = os.getenv("GROK_API_KEY")
    all_posts: list[dict] = []

    if not api_key:
        log.warning("GROK_API_KEY not set — skipping Grok queries")
    else:
        max_accounts = 30
        selected = accounts[:max_accounts]
        log.info("Querying %d accounts individually with concurrency", len(selected))
        sem = asyncio.Semaphore(5)

        async def _query_with_semaphore(handle: str) -> list[dict]:
            async with sem:
                return await _query_single_account(handle, hours=1)

        async def _query_with_timeout(handle: str) -> list[dict]:
            try:
                return await asyncio.wait_for(
                    _query_with_semaphore(handle), timeout=30.0,
                )
            except asyncio.TimeoutError:
                log.warning("Grok query timed out for @%s", handle)
                return []

        tasks = [_query_with_timeout(h) for h in selected]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, list):
                all_posts.extend(result)
            elif isinstance(result, Exception):
                log.warning("Grok query failed: %s", result)

    if not all_posts:
        log.info("No posts returned from any accounts")
        return {"keywords": [], "account_posts": [], "matches": [], "no_match_keywords": []}

    keywords = extract_narrative_keywords(all_posts)
    log.info("Extracted %d narrative keywords from %d posts", len(keywords), len(all_posts))

    # Build keyword → account mapping for display
    keyword_accounts: dict[str, set[str]] = {}
    for kw in keywords[:30]:
        kw_lower = kw.lower()
        keyword_accounts[kw] = set()
        for post in all_posts:
            if kw_lower in post.get("post_text", "").lower():
                keyword_accounts[kw].add(post.get("handle", ""))

    # Limit display detail to top keywords
    top_keywords = keywords[:20]
    keyword_accounts_display = {k: sorted(v) for k, v in keyword_accounts.items() if k in top_keywords}

    matches = await find_matching_coins(keywords)

    matched_keywords = {m["keyword"] for m in matches}
    no_match = [k for k in top_keywords if k not in matched_keywords]

    total_accounts = {p["handle"] for p in all_posts}

    return {
        "keywords": top_keywords,
        "keyword_accounts": keyword_accounts_display,
        "total_posts": len(all_posts),
        "total_accounts_with_posts": len(total_accounts),
        "account_posts": all_posts[:50],
        "matches": matches,
        "no_match_keywords": no_match,
    }
