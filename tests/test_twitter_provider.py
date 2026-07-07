import asyncio
from datetime import UTC, datetime, timedelta

from src.core.models import SignalSource as SignalSourceEnum, SignalType
from src.signals.base import SignalSource
from src.signals.twitter import (
    TwitterMonitor,
    compute_mention_velocity,
    dedupe_posts_by_id,
    extract_solana_mints,
    extract_ticker_symbols,
    score_author_credibility,
)


MINT_ONE = "So11111111111111111111111111111111111111112"
MINT_TWO = "9xQeWvG816bUx9EPfEZj8uN4gR5iKx7mJt8wS7uP8kM"


def _iso(minutes_ago: int) -> str:
    return (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")


def test_twitter_monitor_implements_signal_source_interface() -> None:
    monitor = TwitterMonitor(bearer_token="test-token")

    assert isinstance(monitor, SignalSource)
    assert monitor.name == "twitter"


def test_twitter_monitor_normalizes_fake_payloads_into_valid_signals() -> None:
    payload = {
        "data": [
            {
                "id": "tweet-1",
                "author_id": "user-1",
                "created_at": _iso(2),
                "text": f"Buying $PEPE now. CA {MINT_ONE}",
            },
            {
                "id": "tweet-2",
                "author_id": "user-2",
                "created_at": _iso(1),
                "text": f"Watching $PEPE and {MINT_ONE} closely.",
            },
            {
                "id": "tweet-3",
                "author_id": "user-3",
                "created_at": _iso(0),
                "text": f"Avoid this one, looks shaky: {MINT_TWO}",
            },
        ],
        "includes": {
            "users": [
                {
                    "id": "user-1",
                    "username": "alpha",
                    "verified": True,
                    "created_at": _iso(365),
                    "public_metrics": {"followers_count": 12000},
                },
                {
                    "id": "user-2",
                    "username": "beta",
                    "created_at": _iso(180),
                    "public_metrics": {"followers_count": 4500},
                },
                {
                    "id": "user-3",
                    "username": "gamma",
                    "created_at": _iso(10),
                    "public_metrics": {"followers_count": 300},
                },
            ]
        },
    }

    async def fake_fetcher(_client: object, _auth_mode: str, _query: str, _limit: int) -> object:
        return payload

    async def run() -> list:
        monitor = TwitterMonitor(bearer_token="test-token", fetcher=fake_fetcher)
        signals = await monitor.poll()
        await monitor.stop()
        return signals

    signals = asyncio.run(run())

    assert len(signals) == 2
    signals_by_mint = {signal.mint_address: signal for signal in signals}

    buy_signal = signals_by_mint[MINT_ONE]
    warning_signal = signals_by_mint[MINT_TWO]

    assert buy_signal.source == SignalSourceEnum.TWITTER
    assert buy_signal.type == SignalType.BUY
    assert buy_signal.payload["tickers"] == ["PEPE"]
    assert buy_signal.payload["mention_count_window"] == 2
    assert buy_signal.payload["unique_accounts_window"] == 2
    assert buy_signal.message == f"Twitter buy-call momentum ($PEPE) for {MINT_ONE} by @beta"
    assert 0.0 <= buy_signal.confidence <= 1.0
    assert buy_signal.payload["credibility_tier"] in {"A", "B", "S"}
    assert buy_signal.payload["credibility_avg_score"] > 0.5

    assert warning_signal.source == SignalSourceEnum.TWITTER
    assert warning_signal.type == SignalType.MENTION
    assert warning_signal.payload["text_type"] == "warning"
    assert warning_signal.message == f"Twitter warning chatter for {MINT_TWO} by @gamma"


def test_twitter_monitor_without_api_keys_returns_no_signals() -> None:
    async def run() -> list:
        monitor = TwitterMonitor(bearer_token="", grok_api_key="")
        signals = await monitor.poll()
        await monitor.stop()
        return signals

    assert asyncio.run(run()) == []


def test_mention_velocity_scores_faster_growth_higher() -> None:
    now = datetime(2026, 7, 7, 5, 0, tzinfo=UTC)
    fast = [now - timedelta(minutes=2), now - timedelta(minutes=1), now]
    slow = [now - timedelta(minutes=10), now - timedelta(minutes=5), now]

    assert compute_mention_velocity(fast) > compute_mention_velocity(slow)


def test_twitter_monitor_deduplicates_by_post_id() -> None:
    payload = {
        "data": [
            {
                "id": "tweet-1",
                "author_id": "user-1",
                "created_at": _iso(1),
                "text": f"Buy alert {MINT_ONE}",
            },
            {
                "id": "tweet-1",
                "author_id": "user-1",
                "created_at": _iso(1),
                "text": f"Buy alert {MINT_ONE}",
            },
        ]
    }

    async def fake_fetcher(_client: object, _auth_mode: str, _query: str, _limit: int) -> object:
        return payload

    async def run() -> tuple[list, list]:
        monitor = TwitterMonitor(bearer_token="test-token", fetcher=fake_fetcher)
        first_batch = await monitor.poll()
        second_batch = await monitor.poll()
        await monitor.stop()
        return first_batch, second_batch

    first_batch, second_batch = asyncio.run(run())

    assert len(first_batch) == 1
    assert first_batch[0].payload["mention_count_window"] == 1
    assert second_batch == []


def test_symbol_and_solana_mint_extraction_helpers_work() -> None:
    text = f"$pepe and $BONK both mentioned with {MINT_ONE} twice {MINT_ONE}"

    assert extract_ticker_symbols(text) == ["PEPE", "BONK"]
    assert extract_solana_mints(text) == [MINT_ONE]


def test_dedupe_posts_helper_keeps_first_post_per_id() -> None:
    payload = {
        "data": [
            {
                "id": "tweet-1",
                "author_id": "user-1",
                "created_at": _iso(1),
                "text": f"Observation {MINT_ONE}",
            },
            {
                "id": "tweet-1",
                "author_id": "user-2",
                "created_at": _iso(0),
                "text": f"Observation changed {MINT_TWO}",
            },
        ]
    }
    monitor = TwitterMonitor(bearer_token="test-token")

    posts = monitor._normalize_posts(payload)
    deduped = dedupe_posts_by_id(posts)

    assert len(deduped) == 1
    assert deduped[0].post_id == "tweet-1"
    assert deduped[0].text == f"Observation {MINT_ONE}"


def test_high_follower_verified_author_scores_higher_than_low_credibility_author() -> None:
    observed_at = datetime.now(UTC)
    high = score_author_credibility(
        follower_count=150_000,
        verified=True,
        account_created_at=observed_at - timedelta(days=800),
        observed_at=observed_at,
    )
    low = score_author_credibility(
        follower_count=45,
        verified=False,
        account_created_at=observed_at - timedelta(days=7),
        observed_at=observed_at,
        spam_flags=("bot_flag", "high_spam_score"),
        duplicate_posts=3,
        total_posts=3,
    )

    assert high.score > low.score
    assert high.tier in {"S", "A"}
    assert low.tier == "C"


def test_unknown_metadata_degrades_neutrally() -> None:
    observed_at = datetime.now(UTC)
    result = score_author_credibility(
        follower_count=0,
        verified=None,
        account_created_at=None,
        observed_at=observed_at,
    )

    assert result.score == 0.5
    assert result.tier == "unknown"


def test_spammy_repeated_posts_do_not_dominate_score() -> None:
    payload = {
        "data": [
            {
                "id": "tweet-1",
                "author_id": "spam-1",
                "created_at": _iso(4),
                "text": f"Buy now $PEPE {MINT_ONE}",
                "is_bot": True,
                "spam_score": 0.95,
            },
            {
                "id": "tweet-2",
                "author_id": "spam-1",
                "created_at": _iso(3),
                "text": f"Buy now $PEPE {MINT_ONE}",
                "is_bot": True,
                "spam_score": 0.95,
            },
            {
                "id": "tweet-3",
                "author_id": "spam-1",
                "created_at": _iso(2),
                "text": f"Buy now $PEPE {MINT_ONE}",
                "is_bot": True,
                "spam_score": 0.95,
            },
            {
                "id": "tweet-4",
                "author_id": "real-1",
                "created_at": _iso(1),
                "text": f"Watching $PEPE and {MINT_ONE} closely.",
            },
        ],
        "includes": {
            "users": [
                {
                    "id": "spam-1",
                    "username": "spamlord",
                    "created_at": _iso(15),
                    "public_metrics": {"followers_count": 50000},
                },
                {
                    "id": "real-1",
                    "username": "builder",
                    "verified": True,
                    "created_at": _iso(600),
                    "public_metrics": {"followers_count": 12000},
                },
            ]
        },
    }

    async def fake_fetcher(_client: object, _auth_mode: str, _query: str, _limit: int) -> object:
        return payload

    async def run() -> list:
        monitor = TwitterMonitor(bearer_token="test-token", fetcher=fake_fetcher)
        signals = await monitor.poll()
        await monitor.stop()
        return signals

    signal = asyncio.run(run())[0]

    assert signal.payload["mention_count_window"] == 4
    assert signal.payload["effective_mentions_window"] < 3.0
    assert signal.payload["dominant_author_share"] == 0.75
    assert signal.payload["credibility_by_author"]["spam-1"]["tier"] == "C"


def test_unique_account_diversity_still_matters() -> None:
    single_author_payload = {
        "data": [
            {
                "id": "solo-1",
                "author_id": "solo",
                "created_at": _iso(2),
                "text": f"Buying $PEPE now. CA {MINT_ONE}",
            },
            {
                "id": "solo-2",
                "author_id": "solo",
                "created_at": _iso(1),
                "text": f"Watching $PEPE and {MINT_ONE} closely.",
            },
        ],
        "includes": {
            "users": [
                {
                    "id": "solo",
                    "username": "solo",
                    "verified": True,
                    "created_at": _iso(700),
                    "public_metrics": {"followers_count": 25000},
                }
            ]
        },
    }
    diverse_payload = {
        "data": [
            {
                "id": "div-1",
                "author_id": "user-1",
                "created_at": _iso(2),
                "text": f"Buying $PEPE now. CA {MINT_ONE}",
            },
            {
                "id": "div-2",
                "author_id": "user-2",
                "created_at": _iso(1),
                "text": f"Watching $PEPE and {MINT_ONE} closely.",
            },
        ],
        "includes": {
            "users": [
                {
                    "id": "user-1",
                    "username": "alpha",
                    "verified": True,
                    "created_at": _iso(700),
                    "public_metrics": {"followers_count": 25000},
                },
                {
                    "id": "user-2",
                    "username": "beta",
                    "created_at": _iso(400),
                    "public_metrics": {"followers_count": 9000},
                },
            ]
        },
    }

    async def single_fetcher(_client: object, _auth_mode: str, _query: str, _limit: int) -> object:
        return single_author_payload

    async def diverse_fetcher(_client: object, _auth_mode: str, _query: str, _limit: int) -> object:
        return diverse_payload

    async def run(fetcher) -> float:
        monitor = TwitterMonitor(bearer_token="test-token", fetcher=fetcher)
        signals = await monitor.poll()
        await monitor.stop()
        return signals[0].confidence

    single_confidence = asyncio.run(run(single_fetcher))
    diverse_confidence = asyncio.run(run(diverse_fetcher))

    assert diverse_confidence > single_confidence
