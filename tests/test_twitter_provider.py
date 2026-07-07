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
                    "public_metrics": {"followers_count": 12000},
                },
                {
                    "id": "user-2",
                    "username": "beta",
                    "public_metrics": {"followers_count": 4500},
                },
                {
                    "id": "user-3",
                    "username": "gamma",
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
