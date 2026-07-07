"""Twitter/X signal monitor with deterministic parsing and safe API degradation.

The official Twitter recent-search API is the most stable source for structured post search
when a bearer token is available. `GROK_API_KEY` is recognized as a future fallback surface,
but this module intentionally avoids guessing at a chat-style search contract; in grok-only
mode it degrades safely unless a custom fetcher is injected for tests or future integration.
"""

from __future__ import annotations

import math
import os
import re
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import httpx

from src.core.models import Signal
from src.core.models import SignalSource as SignalSourceEnum
from src.core.models import SignalType
from src.core.models import utc_now
from src.signals.base import SignalSource

AuthMode = Literal["twitter", "grok", "none"]
PostFetcher = Callable[[httpx.AsyncClient | None, AuthMode, str, int], Awaitable[object]]

TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
DEFAULT_QUERY = "(solana OR pumpfun OR pump.fun OR memecoin) (CA OR contract OR mint OR $) -is:retweet lang:en"
BUY_KEYWORDS = ("buy", "ape", "entry", "long", "sending", "moon", "runner")
WARNING_KEYWORDS = ("avoid", "scam", "rug", "dump", "exit", "warning", "sell")
TICKER_PATTERN = re.compile(r"(?<!\w)\$([A-Za-z][A-Za-z0-9]{1,14})\b")
SOLANA_MINT_PATTERN = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")


@dataclass(slots=True)
class TwitterPost:
    post_id: str
    text: str
    author_id: str
    author_handle: str | None
    follower_count: int
    created_at: datetime


@dataclass(slots=True)
class MentionRecord:
    post_id: str
    author_id: str
    follower_count: int
    observed_at: datetime
    text_type: str
    tickers: tuple[str, ...]


def extract_ticker_symbols(text: str) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for match in TICKER_PATTERN.finditer(text):
        symbol = match.group(1).upper()
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def extract_solana_mints(text: str) -> list[str]:
    seen: set[str] = set()
    mints: list[str] = []
    for match in SOLANA_MINT_PATTERN.finditer(text):
        mint = match.group(0)
        if mint in seen:
            continue
        seen.add(mint)
        mints.append(mint)
    return mints


def classify_text_type(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in WARNING_KEYWORDS):
        return "warning"
    if any(keyword in lowered for keyword in BUY_KEYWORDS):
        return "buy_call"
    return "observation"


def dedupe_posts_by_id(posts: Sequence[TwitterPost]) -> list[TwitterPost]:
    deduped: list[TwitterPost] = []
    seen_ids: set[str] = set()
    for post in posts:
        if post.post_id in seen_ids:
            continue
        seen_ids.add(post.post_id)
        deduped.append(post)
    return deduped


def compute_mention_velocity(
    timestamps: Sequence[datetime],
    window: timedelta = timedelta(minutes=15),
) -> float:
    if not timestamps:
        return 0.0

    sorted_times = sorted(timestamps)
    mention_count = len(sorted_times)
    window_minutes = max(window.total_seconds() / 60, 1.0)
    elapsed_minutes = max((sorted_times[-1] - sorted_times[0]).total_seconds() / 60, 0.0)
    effective_minutes = max(elapsed_minutes, window_minutes / mention_count)
    return mention_count / max(effective_minutes, 1e-6)


def score_signal_strength(
    *,
    velocity_per_minute: float,
    unique_accounts: int,
    text_type: str,
    follower_count: int,
    mention_count: int,
) -> float:
    velocity_score = min(velocity_per_minute / 1.5, 1.0) * 0.4
    unique_score = min(unique_accounts / 5, 1.0) * 0.2
    diversity_score = min(unique_accounts / max(mention_count, 1), 1.0) * 0.15
    follower_score = min(math.log10(max(follower_count, 0) + 1) / 6, 1.0) * 0.15
    text_score = {"buy_call": 0.2, "observation": 0.1, "warning": 0.0}.get(text_type, 0.0)

    score = velocity_score + unique_score + diversity_score + follower_score + text_score
    if text_type == "warning":
        score *= 0.5
    return min(max(score, 0.0), 1.0)


async def _default_fetch_posts(
    client: httpx.AsyncClient | None,
    auth_mode: AuthMode,
    query: str,
    limit: int,
) -> object:
    if auth_mode != "twitter" or client is None:
        return []

    response = await client.get(
        TWITTER_SEARCH_URL,
        params={
            "query": query,
            "max_results": min(max(limit, 10), 100),
            "expansions": "author_id",
            "tweet.fields": "author_id,created_at,public_metrics,text",
            "user.fields": "public_metrics,username",
        },
    )
    response.raise_for_status()
    return response.json()


class TwitterMonitor(SignalSource):
    def __init__(
        self,
        *,
        bearer_token: str | None = None,
        grok_api_key: str | None = None,
        query: str = DEFAULT_QUERY,
        limit: int = 25,
        timeout_s: float = 10.0,
        window: timedelta = timedelta(minutes=15),
        fetcher: PostFetcher | None = None,
    ) -> None:
        self._bearer_token = (bearer_token or os.getenv("TWITTER_BEARER_TOKEN", "")).strip()
        self._grok_api_key = (grok_api_key or os.getenv("GROK_API_KEY", "")).strip()
        self._query = query
        self._limit = max(limit, 1)
        self._timeout_s = timeout_s
        self._window = window
        self._fetcher = fetcher or _default_fetch_posts

        self._client: httpx.AsyncClient | None = None
        self._started = False
        self._seen_post_ids: set[str] = set()
        self._mention_history: dict[str, list[MentionRecord]] = defaultdict(list)

    @property
    def name(self) -> str:
        return "twitter"

    async def start(self) -> None:
        if self._started:
            return

        if self._auth_mode == "twitter" and self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout_s,
                headers={"Authorization": f"Bearer {self._bearer_token}"},
            )

        self._started = True

    async def stop(self) -> None:
        self._started = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def poll(self) -> list[Signal]:
        if self._auth_mode == "none":
            return []
        if not self._started:
            await self.start()

        try:
            payload = await self._fetcher(self._client, self._auth_mode, self._query, self._limit)
        except Exception:
            return []

        posts = self._normalize_posts(payload)
        fresh_posts = [post for post in dedupe_posts_by_id(posts) if post.post_id not in self._seen_post_ids]
        for post in fresh_posts:
            self._seen_post_ids.add(post.post_id)

        return self._build_signals(fresh_posts)

    @property
    def _auth_mode(self) -> AuthMode:
        if self._bearer_token:
            return "twitter"
        if self._grok_api_key:
            return "grok"
        return "none"

    def _normalize_posts(self, payload: object) -> list[TwitterPost]:
        if isinstance(payload, list):
            return [post for item in payload if (post := self._post_from_mapping(item)) is not None]

        if not isinstance(payload, Mapping):
            return []

        author_lookup: dict[str, Mapping[str, object]] = {}
        includes = payload.get("includes")
        if isinstance(includes, Mapping):
            users = includes.get("users")
            if isinstance(users, list):
                for user in users:
                    if not isinstance(user, Mapping):
                        continue
                    user_id = user.get("id")
                    if isinstance(user_id, str) and user_id:
                        author_lookup[user_id] = user

        posts: list[TwitterPost] = []
        data = payload.get("data")
        if isinstance(data, list):
            for item in data:
                post = self._post_from_mapping(item, author_lookup)
                if post is not None:
                    posts.append(post)
        else:
            post = self._post_from_mapping(payload, author_lookup)
            if post is not None:
                posts.append(post)

        return posts

    def _post_from_mapping(
        self,
        payload: object,
        author_lookup: Mapping[str, Mapping[str, object]] | None = None,
    ) -> TwitterPost | None:
        if not isinstance(payload, Mapping):
            return None

        post_id = self._string_field(payload, "id", "tweet_id", "post_id")
        text = self._string_field(payload, "text", "body", "message")
        if not post_id or not text:
            return None

        author_id = self._string_field(payload, "author_id", "authorId", "user_id") or "unknown"
        author = author_lookup.get(author_id, {}) if author_lookup else {}
        author_handle = self._string_field(author, "username", "handle") or self._string_field(
            payload,
            "username",
            "author_handle",
        )
        follower_count = self._follower_count(payload, author)
        created_at = self._parse_datetime(payload.get("created_at")) or utc_now()

        return TwitterPost(
            post_id=post_id,
            text=text,
            author_id=author_id,
            author_handle=author_handle,
            follower_count=follower_count,
            created_at=created_at,
        )

    def _build_signals(self, posts: Sequence[TwitterPost]) -> list[Signal]:
        if self._auth_mode == "grok":
            return []

        touched_mints: set[str] = set()
        latest_posts: dict[str, TwitterPost] = {}
        current_batch_types: dict[str, list[str]] = defaultdict(list)
        current_batch_tickers: dict[str, set[str]] = defaultdict(set)

        for post in posts:
            mints = extract_solana_mints(post.text)
            if not mints:
                continue

            text_type = classify_text_type(post.text)
            tickers = tuple(extract_ticker_symbols(post.text))
            for mint in mints:
                touched_mints.add(mint)
                latest_posts[mint] = post
                current_batch_types[mint].append(text_type)
                current_batch_tickers[mint].update(tickers)
                self._mention_history[mint].append(
                    MentionRecord(
                        post_id=post.post_id,
                        author_id=post.author_id,
                        follower_count=post.follower_count,
                        observed_at=post.created_at,
                        text_type=text_type,
                        tickers=tickers,
                    )
                )

        signals: list[Signal] = []
        for mint in touched_mints:
            history = self._prune_history(mint)
            if not history:
                continue

            unique_accounts = len({record.author_id for record in history})
            mention_count = len(history)
            velocity = compute_mention_velocity(
                [record.observed_at for record in history],
                self._window,
            )
            max_followers = max(record.follower_count for record in history)
            text_type = self._strongest_text_type(current_batch_types[mint])
            confidence = score_signal_strength(
                velocity_per_minute=velocity,
                unique_accounts=unique_accounts,
                text_type=text_type,
                follower_count=max_followers,
                mention_count=mention_count,
            )
            signal_type = SignalType.BUY if text_type == "buy_call" else SignalType.MENTION
            latest_post = latest_posts[mint]
            tickers = sorted(current_batch_tickers[mint])
            payload = {
                "query": self._query,
                "auth_mode": self._auth_mode,
                "tickers": tickers,
                "mention_count_window": mention_count,
                "unique_accounts_window": unique_accounts,
                "velocity_per_minute": round(velocity, 4),
                "text_type": text_type,
                "max_follower_count": max_followers,
                "post_ids": [record.post_id for record in history],
                "author_handles": sorted(
                    {
                        latest_post.author_handle
                        for latest_post in latest_posts.values()
                        if latest_post.author_handle
                    }
                ),
            }
            signals.append(
                Signal(
                    source=SignalSourceEnum.TWITTER,
                    type=signal_type,
                    mint_address=mint,
                    confidence=confidence,
                    weight=round(1.0 + min(math.log10(max_followers + 1) / 3, 1.0), 3),
                    message=self._build_message(mint, tickers, text_type, latest_post),
                    payload=payload,
                    observed_at=latest_post.created_at,
                )
            )

        return signals

    def _prune_history(self, mint: str) -> list[MentionRecord]:
        cutoff = utc_now() - self._window
        history = [record for record in self._mention_history.get(mint, []) if record.observed_at >= cutoff]
        self._mention_history[mint] = history
        return history

    def _strongest_text_type(self, text_types: Sequence[str]) -> str:
        if any(text_type == "buy_call" for text_type in text_types):
            return "buy_call"
        if any(text_type == "warning" for text_type in text_types):
            return "warning"
        return "observation"

    def _build_message(
        self,
        mint: str,
        tickers: Sequence[str],
        text_type: str,
        latest_post: TwitterPost,
    ) -> str:
        ticker_text = f" (${tickers[0]})" if tickers else ""
        handle_text = f" by @{latest_post.author_handle}" if latest_post.author_handle else ""
        prefix = {
            "buy_call": "Twitter buy-call momentum",
            "warning": "Twitter warning chatter",
            "observation": "Twitter mention velocity",
        }[text_type]
        return f"{prefix}{ticker_text} for {mint}{handle_text}"

    def _string_field(self, payload: Mapping[str, object], *field_names: str) -> str:
        for field_name in field_names:
            value = payload.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _follower_count(self, payload: Mapping[str, object], author: Mapping[str, object]) -> int:
        for candidate in (
            payload.get("follower_count"),
            payload.get("followers"),
            self._metrics_followers(payload.get("public_metrics")),
            author.get("follower_count"),
            self._metrics_followers(author.get("public_metrics")),
        ):
            if isinstance(candidate, int):
                return max(candidate, 0)
        return 0

    def _metrics_followers(self, value: object) -> int | None:
        if not isinstance(value, Mapping):
            return None
        followers = value.get("followers_count")
        if isinstance(followers, int):
            return followers
        return None

    def _parse_datetime(self, value: object) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)


class TwitterSignalSource(TwitterMonitor):
    """Backward-compatible alias for the original placeholder class name."""
