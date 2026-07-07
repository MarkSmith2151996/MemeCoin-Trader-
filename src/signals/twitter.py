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


@dataclass(frozen=True, slots=True)
class CredibilityResult:
    score: float
    tier: str
    details: dict[str, float | int | str | bool | None]


@dataclass(slots=True)
class TwitterPost:
    post_id: str
    text: str
    author_id: str
    author_handle: str | None
    follower_count: int
    created_at: datetime
    author_created_at: datetime | None = None
    verified: bool | None = None
    spam_flags: tuple[str, ...] = ()


@dataclass(slots=True)
class MentionRecord:
    post_id: str
    author_id: str
    follower_count: int
    observed_at: datetime
    text_type: str
    tickers: tuple[str, ...]
    content_fingerprint: str


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


def normalize_post_fingerprint(text: str) -> str:
    normalized = TICKER_PATTERN.sub("$TICKER", text.upper())
    normalized = SOLANA_MINT_PATTERN.sub("MINT", normalized)
    normalized = re.sub(r"https?://\S+", "URL", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def score_author_credibility(
    *,
    follower_count: int,
    verified: bool | None,
    account_created_at: datetime | None,
    observed_at: datetime,
    spam_flags: Sequence[str] = (),
    duplicate_posts: int = 0,
    total_posts: int = 1,
) -> CredibilityResult:
    metadata_known = verified is not None or account_created_at is not None or bool(spam_flags) or follower_count > 0
    score = 0.5

    follower_bonus = min(math.log10(max(follower_count, 0) + 1) / 6, 1.0) * 0.2
    score += follower_bonus

    if verified is True:
        score += 0.15

    account_age_days: float | None = None
    if account_created_at is not None:
        account_age_days = max((observed_at - account_created_at).total_seconds() / 86400, 0.0)
        if account_age_days >= 365:
            score += 0.1
        elif account_age_days >= 90:
            score += 0.05
        elif account_age_days < 30:
            score -= 0.1

    spam_penalty = min(len(spam_flags), 2) * 0.15
    score -= spam_penalty

    duplicate_ratio = duplicate_posts / max(total_posts, 1)
    duplicate_penalty = 0.0
    if duplicate_posts >= 3 and duplicate_ratio >= 0.75:
        duplicate_penalty = 0.2
    elif duplicate_posts >= 2 and duplicate_ratio >= 0.5:
        duplicate_penalty = 0.1
    score -= duplicate_penalty

    score = min(max(score, 0.05), 0.95)

    if not metadata_known:
        tier = "unknown"
    elif score >= 0.85:
        tier = "S"
    elif score >= 0.72:
        tier = "A"
    elif score >= 0.58:
        tier = "B"
    else:
        tier = "C"

    return CredibilityResult(
        score=round(score, 4),
        tier=tier,
        details={
            "follower_count": max(follower_count, 0),
            "verified": verified,
            "account_age_days": round(account_age_days, 2) if account_age_days is not None else None,
            "spam_flags": list(spam_flags),
            "duplicate_posts": duplicate_posts,
            "duplicate_ratio": round(duplicate_ratio, 4),
        },
    )


def score_signal_strength(
    *,
    velocity_per_minute: float,
    unique_accounts: int,
    text_type: str,
    follower_count: float,
    mention_count: float,
    avg_credibility: float,
    effective_unique_accounts: float,
    dominant_author_share: float,
) -> float:
    velocity_score = min(velocity_per_minute / 1.5, 1.0) * 0.4
    unique_score = min(unique_accounts / 5, 1.0) * 0.15
    weighted_unique_score = min(effective_unique_accounts / 3, 1.0) * 0.1
    diversity_score = min(unique_accounts / max(mention_count, 1.0), 1.0) * 0.1
    follower_score = min(math.log10(max(follower_count, 0) + 1) / 6, 1.0) * 0.15
    credibility_score = min(max(avg_credibility, 0.0), 1.0) * 0.1
    concentration_penalty = min(max(dominant_author_share - 0.5, 0.0), 0.5) * 0.3
    text_score = {"buy_call": 0.2, "observation": 0.1, "warning": 0.0}.get(text_type, 0.0)

    score = (
        velocity_score
        + unique_score
        + weighted_unique_score
        + diversity_score
        + follower_score
        + credibility_score
        + text_score
        - concentration_penalty
    )
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
            "user.fields": "created_at,public_metrics,username,verified",
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
        author_created_at = self._parse_datetime(author.get("created_at")) or self._parse_datetime(
            payload.get("author_created_at")
        )
        verified = self._verified_flag(payload, author)
        spam_flags = tuple(self._spam_flags(payload, author))

        return TwitterPost(
            post_id=post_id,
            text=text,
            author_id=author_id,
            author_handle=author_handle,
            follower_count=follower_count,
            created_at=created_at,
            author_created_at=author_created_at,
            verified=verified,
            spam_flags=spam_flags,
        )

    def _build_signals(self, posts: Sequence[TwitterPost]) -> list[Signal]:
        if self._auth_mode == "grok":
            return []

        touched_mints: set[str] = set()
        latest_posts: dict[str, TwitterPost] = {}
        current_batch_types: dict[str, list[str]] = defaultdict(list)
        current_batch_tickers: dict[str, set[str]] = defaultdict(set)
        author_posts: dict[str, TwitterPost] = {}

        for post in posts:
            mints = extract_solana_mints(post.text)
            if not mints:
                continue

            text_type = classify_text_type(post.text)
            tickers = tuple(extract_ticker_symbols(post.text))
            fingerprint = normalize_post_fingerprint(post.text)
            author_posts[post.author_id] = post
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
                        content_fingerprint=fingerprint,
                    )
                )

        signals: list[Signal] = []
        for mint in touched_mints:
            history = self._prune_history(mint)
            if not history:
                continue

            unique_accounts = len({record.author_id for record in history})
            mention_count = len(history)
            records_by_author: dict[str, list[MentionRecord]] = defaultdict(list)
            for record in history:
                records_by_author[record.author_id].append(record)

            credibility_by_author: dict[str, CredibilityResult] = {}
            capped_weighted_mentions = 0.0
            effective_unique_accounts = 0.0
            weighted_follower_total = 0.0
            max_author_mentions = 0
            for author_id, author_history in records_by_author.items():
                max_author_mentions = max(max_author_mentions, len(author_history))
                duplicate_posts = len(author_history) - len({record.content_fingerprint for record in author_history})
                author_post = author_posts.get(author_id)
                credibility = score_author_credibility(
                    follower_count=author_post.follower_count if author_post is not None else 0,
                    verified=author_post.verified if author_post is not None else None,
                    account_created_at=author_post.author_created_at if author_post is not None else None,
                    observed_at=max(record.observed_at for record in author_history),
                    spam_flags=author_post.spam_flags if author_post is not None else (),
                    duplicate_posts=duplicate_posts,
                    total_posts=len(author_history),
                )
                credibility_by_author[author_id] = credibility
                capped_weighted_mentions += min(len(author_history), 2) * credibility.score
                effective_unique_accounts += credibility.score
                weighted_follower_total += (author_post.follower_count if author_post is not None else 0) * credibility.score

            velocity = compute_mention_velocity(
                [record.observed_at for record in history],
                self._window,
            )
            max_followers = max(record.follower_count for record in history)
            avg_credibility = sum(result.score for result in credibility_by_author.values()) / max(
                len(credibility_by_author), 1
            )
            dominant_author_share = max_author_mentions / max(mention_count, 1)
            text_type = self._strongest_text_type(current_batch_types[mint])
            confidence = score_signal_strength(
                velocity_per_minute=velocity,
                unique_accounts=unique_accounts,
                text_type=text_type,
                follower_count=max(weighted_follower_total, max_followers),
                mention_count=max(capped_weighted_mentions, 1.0),
                avg_credibility=avg_credibility,
                effective_unique_accounts=effective_unique_accounts,
                dominant_author_share=dominant_author_share,
            )
            signal_type = SignalType.BUY if text_type == "buy_call" else SignalType.MENTION
            latest_post = latest_posts[mint]
            tickers = sorted(current_batch_tickers[mint])
            author_handles = sorted(
                {
                    post.author_handle
                    for post in author_posts.values()
                    if post.author_handle
                }
            )
            top_credibility = max(
                credibility_by_author.values(),
                key=lambda result: result.score,
                default=CredibilityResult(score=0.5, tier="unknown", details={}),
            )
            payload = {
                "query": self._query,
                "auth_mode": self._auth_mode,
                "tickers": tickers,
                "mention_count_window": mention_count,
                "unique_accounts_window": unique_accounts,
                "velocity_per_minute": round(velocity, 4),
                "text_type": text_type,
                "max_follower_count": max_followers,
                "effective_mentions_window": round(capped_weighted_mentions, 4),
                "effective_unique_accounts": round(effective_unique_accounts, 4),
                "dominant_author_share": round(dominant_author_share, 4),
                "credibility_avg_score": round(avg_credibility, 4),
                "credibility_tier": top_credibility.tier,
                "credibility_by_author": {
                    author_id: {
                        "score": result.score,
                        "tier": result.tier,
                        **result.details,
                    }
                    for author_id, result in credibility_by_author.items()
                },
                "post_ids": [record.post_id for record in history],
                "author_handles": author_handles,
            }
            signals.append(
                Signal(
                    source=SignalSourceEnum.TWITTER,
                    type=signal_type,
                    mint_address=mint,
                    confidence=confidence,
                    weight=round(
                        1.0 + min(math.log10(max(weighted_follower_total, max_followers) + 1) / 3, 1.0) * avg_credibility,
                        3,
                    ),
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

    def _verified_flag(self, payload: Mapping[str, object], author: Mapping[str, object]) -> bool | None:
        for candidate in (payload.get("verified"), author.get("verified")):
            if isinstance(candidate, bool):
                return candidate
        return None

    def _spam_flags(self, payload: Mapping[str, object], author: Mapping[str, object]) -> list[str]:
        flags: list[str] = []
        candidate_fields = (
            (payload.get("is_bot"), "bot_flag"),
            (author.get("is_bot"), "bot_flag"),
            (payload.get("is_spam"), "spam_flag"),
            (author.get("is_spam"), "spam_flag"),
            (payload.get("default_profile_image"), "default_avatar"),
            (author.get("default_profile_image"), "default_avatar"),
        )
        for value, label in candidate_fields:
            if value is True and label not in flags:
                flags.append(label)

        for value in (payload.get("spam_score"), author.get("spam_score")):
            if isinstance(value, (int, float)) and value >= 0.8 and "high_spam_score" not in flags:
                flags.append("high_spam_score")

        return flags

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
