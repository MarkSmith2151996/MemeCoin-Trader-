import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.core.models import Signal, SignalSource as SignalSourceEnum, SignalType
from src.signals.aggregator import SignalAggregator
from src.signals.base import SignalSource


BASE_TIME = datetime(2026, 7, 7, tzinfo=UTC)


class FakeSource(SignalSource):
    def __init__(self, name: str, signals: list[Signal], *, poll_error: Exception | None = None) -> None:
        self._name = name
        self._signals = signals
        self._poll_error = poll_error
        self.started = False
        self.stopped = False

    @property
    def name(self) -> str:
        return self._name

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def poll(self) -> list[Signal]:
        if self._poll_error is not None:
            raise self._poll_error
        return list(self._signals)


def build_signal(
    *,
    mint: str,
    source: SignalSourceEnum,
    confidence: float,
    observed_at: datetime,
    weight: float = 1.0,
) -> Signal:
    return Signal(
        source=source,
        type=SignalType.BUY,
        mint_address=mint,
        confidence=confidence,
        weight=weight,
        observed_at=observed_at,
        payload={"mint": mint, "source": source.value},
    )


def test_aggregator_polls_multiple_sources_and_returns_combined_ranked_signals() -> None:
    async def run() -> None:
        high = build_signal(
            mint="mint-high",
            source=SignalSourceEnum.WHALE_TRACKER,
            confidence=0.9,
            observed_at=BASE_TIME,
        )
        low = build_signal(
            mint="mint-low",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.4,
            observed_at=BASE_TIME + timedelta(seconds=1),
        )
        source_a = FakeSource("a", [low])
        source_b = FakeSource("b", [high])
        aggregator = SignalAggregator([source_a, source_b])

        await aggregator.start()
        ranked = await aggregator.poll_all()
        await aggregator.stop()

        assert source_a.started is True
        assert source_b.started is True
        assert source_a.stopped is True
        assert source_b.stopped is True
        assert [signal.mint_address for signal in ranked] == ["mint-high", "mint-low"]

    asyncio.run(run())


def test_same_mint_inside_window_becomes_one_boosted_signal() -> None:
    async def run() -> None:
        signal_a = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.6,
            observed_at=BASE_TIME,
        )
        signal_b = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.WHALE_TRACKER,
            confidence=0.4,
            observed_at=BASE_TIME + timedelta(seconds=30),
        )
        aggregator = SignalAggregator([FakeSource("a", [signal_a]), FakeSource("b", [signal_b])])

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        assert ranked[0].mint_address == "mint-1"
        assert ranked[0].confidence == pytest.approx(0.9)
        assert ranked[0].weight == 1.0
        assert ranked[0].payload["source_count"] == 2
        assert len(ranked[0].payload["raw_data"]) == 2

    asyncio.run(run())


def test_same_mint_outside_window_remains_separate() -> None:
    async def run() -> None:
        early = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.3,
            observed_at=BASE_TIME,
        )
        late = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.WHALE_TRACKER,
            confidence=0.8,
            observed_at=BASE_TIME + timedelta(seconds=301),
        )
        aggregator = SignalAggregator(
            [FakeSource("a", [early]), FakeSource("b", [late])],
            dedup_window_seconds=300,
        )

        ranked = await aggregator.poll_all()

        assert len(ranked) == 2
        assert [signal.confidence for signal in ranked] == [0.8, 0.3]

    asyncio.run(run())


def test_signals_are_ranked_highest_first() -> None:
    async def run() -> None:
        boosted_a = build_signal(
            mint="mint-boost",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.5,
            observed_at=BASE_TIME,
        )
        boosted_b = build_signal(
            mint="mint-boost",
            source=SignalSourceEnum.WHALE_TRACKER,
            confidence=0.5,
            observed_at=BASE_TIME + timedelta(seconds=5),
        )
        single = build_signal(
            mint="mint-single",
            source=SignalSourceEnum.ONCHAIN,
            confidence=0.7,
            observed_at=BASE_TIME + timedelta(seconds=2),
        )
        aggregator = SignalAggregator(
            [
                FakeSource("a", [boosted_a]),
                FakeSource("b", [boosted_b]),
                FakeSource("c", [single]),
            ]
        )

        ranked = await aggregator.poll_all()

        assert [signal.mint_address for signal in ranked] == ["mint-boost", "mint-single"]
        assert ranked[0].confidence == pytest.approx(0.75)
        assert ranked[1].confidence == pytest.approx(0.7)

    asyncio.run(run())


def test_single_source_signal_passes_through_unmodified_strength() -> None:
    async def run() -> None:
        original = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.55,
            observed_at=BASE_TIME,
            weight=0.8,
        )
        aggregator = SignalAggregator([FakeSource("a", [original])])

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        assert ranked[0].confidence == original.confidence
        assert ranked[0].weight == original.weight
        assert ranked[0].message == original.message

    asyncio.run(run())


def test_pump_fun_promotional_identity_adds_ranking_context_without_rejecting_signal() -> None:
    async def run() -> None:
        original = build_signal(
            mint="mint-weak-identity",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.7,
            observed_at=BASE_TIME,
        )
        original.payload.update(
            {
                "name": "READ INSANE FOLLOWERS",
                "symbol": "HORDE",
                "attention_diagnostics": {"metadata_completeness_state": "partial"},
            }
        )
        aggregator = SignalAggregator([FakeSource("pump_fun", [original])])

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        context = ranked[0].payload.get("pump_fun_identity_context")
        assert isinstance(context, dict)
        assert context["weak_identity_name"] is True
        assert "partial_metadata" in context["reasons"]
        assert "weak_identity" in context["reasons"]
        assert ranked[0].confidence == original.confidence
        assert ranked[0].weight == original.weight

    asyncio.run(run())


def test_non_pump_fun_signal_does_not_receive_pump_fun_identity_context() -> None:
    async def run() -> None:
        signal = build_signal(
            mint="mint-onchain",
            source=SignalSourceEnum.ONCHAIN,
            confidence=0.7,
            observed_at=BASE_TIME,
        )
        aggregator = SignalAggregator([FakeSource("onchain", [signal])])

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        assert "pump_fun_identity_context" not in ranked[0].payload

    asyncio.run(run())


def test_empty_source_list_returns_empty_list() -> None:
    async def run() -> None:
        aggregator = SignalAggregator([])

        ranked = await aggregator.poll_all()
        top = await aggregator.get_top_opportunities()

        assert ranked == []
        assert top == []

    asyncio.run(run())


def test_source_failure_does_not_crash_aggregator() -> None:
    async def run() -> None:
        healthy = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.6,
            observed_at=BASE_TIME,
        )
        aggregator = SignalAggregator(
            [
                FakeSource("healthy", [healthy]),
                FakeSource("broken", [], poll_error=RuntimeError("boom")),
            ]
        )

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        assert ranked[0].mint_address == "mint-1"

    asyncio.run(run())


def test_aggregator_reports_source_and_composite_diagnostics() -> None:
    async def run() -> None:
        signal_a = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.6,
            observed_at=BASE_TIME,
        )
        signal_b = build_signal(
            mint="mint-1",
            source=SignalSourceEnum.WHALE_TRACKER,
            confidence=0.5,
            observed_at=BASE_TIME + timedelta(seconds=5),
        )
        aggregator = SignalAggregator(
            [
                FakeSource("pump_fun", [signal_a]),
                FakeSource("whale_tracker", [signal_b]),
                FakeSource("broken", [], poll_error=RuntimeError("boom")),
            ]
        )

        ranked = await aggregator.poll_all()
        diagnostics = aggregator.diagnostics()

        assert len(ranked) == 1
        assert diagnostics["sources_polled"] == ["pump_fun", "whale_tracker", "broken"]
        assert diagnostics["source_signal_counts"] == {"pump_fun": 1, "whale_tracker": 1}
        assert diagnostics["source_failures"] == {"broken": 1}
        assert diagnostics["raw_signal_count"] == 2
        assert diagnostics["composite_opportunities"] == 1
        assert diagnostics["ranked_opportunities"] == 1

    asyncio.run(run())


def test_composite_signal_preserves_and_summarizes_social_credibility_metadata() -> None:
    async def run() -> None:
        twitter_signal = Signal(
            source=SignalSourceEnum.TWITTER,
            type=SignalType.BUY,
            mint_address="mint-social",
            confidence=0.7,
            weight=1.1,
            observed_at=BASE_TIME,
            payload={
                "credibility_tier": "A",
                "credibility_by_author": {
                    "author-1": {
                        "tier": "A",
                        "score": 0.81,
                        "spam_flags": [],
                        "duplicate_posts": 0,
                    },
                    "author-2": {
                        "tier": "C",
                        "score": 0.33,
                        "spam_flags": ["bot_flag"],
                        "duplicate_posts": 2,
                    },
                },
            },
        )
        onchain_signal = build_signal(
            mint="mint-social",
            source=SignalSourceEnum.ONCHAIN,
            confidence=0.5,
            observed_at=BASE_TIME + timedelta(seconds=5),
        )
        aggregator = SignalAggregator(
            [FakeSource("twitter", [twitter_signal]), FakeSource("onchain", [onchain_signal])]
        )

        ranked = await aggregator.poll_all()
        diagnostics = aggregator.diagnostics()

        assert len(ranked) == 1
        assert ranked[0].payload["credibility_tier"] == "A"
        assert ranked[0].payload["social_credibility"] == {
            "highest_tier": "A",
            "unique_accounts": 2,
            "tier_distribution": {"A": 1, "C": 1},
            "spam_flagged_accounts": 1,
            "duplicate_suppression_posts": 2,
        }
        assert diagnostics["social_credibility"] == ranked[0].payload["social_credibility"]

    asyncio.run(run())


def test_aggregator_social_diagnostics_degrade_safely_when_metadata_missing() -> None:
    async def run() -> None:
        twitter_signal = Signal(
            source=SignalSourceEnum.TWITTER,
            type=SignalType.MENTION,
            mint_address="mint-social",
            confidence=0.4,
            observed_at=BASE_TIME,
            payload={"credibility_tier": "unknown"},
        )
        aggregator = SignalAggregator([FakeSource("twitter", [twitter_signal])])

        ranked = await aggregator.poll_all()
        diagnostics = aggregator.diagnostics()

        assert len(ranked) == 1
        assert "social_credibility" not in ranked[0].payload
        assert diagnostics["social_credibility"] == {
            "highest_tier": "unknown",
            "unique_accounts": 0,
            "tier_distribution": {"unknown": 1},
            "spam_flagged_accounts": 0,
            "duplicate_suppression_posts": 0,
        }

    asyncio.run(run())


def test_spammy_social_signals_do_not_inflate_aggregate_diagnostics() -> None:
    async def run() -> None:
        spam_signal_one = Signal(
            source=SignalSourceEnum.TWITTER,
            type=SignalType.BUY,
            mint_address="mint-social",
            confidence=0.5,
            observed_at=BASE_TIME,
            payload={
                "credibility_tier": "C",
                "credibility_by_author": {
                    "spam-author": {
                        "tier": "C",
                        "score": 0.2,
                        "spam_flags": ["bot_flag"],
                        "duplicate_posts": 3,
                    }
                },
            },
        )
        spam_signal_two = Signal(
            source=SignalSourceEnum.TWITTER,
            type=SignalType.BUY,
            mint_address="mint-social",
            confidence=0.45,
            observed_at=BASE_TIME + timedelta(seconds=5),
            payload={
                "credibility_tier": "C",
                "credibility_by_author": {
                    "spam-author": {
                        "tier": "C",
                        "score": 0.2,
                        "spam_flags": ["bot_flag"],
                        "duplicate_posts": 3,
                    }
                },
            },
        )
        aggregator = SignalAggregator(
            [FakeSource("twitter", [spam_signal_one, spam_signal_two])]
        )

        await aggregator.poll_all()
        diagnostics = aggregator.diagnostics()

        assert diagnostics["social_credibility"] == {
            "highest_tier": "C",
            "unique_accounts": 1,
            "tier_distribution": {"C": 1},
            "spam_flagged_accounts": 1,
            "duplicate_suppression_posts": 3,
        }

    asyncio.run(run())


def test_database_logging_is_called_when_sqlite_contract_supports_signals(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "signals.db"
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                CREATE TABLE signals (
                    source TEXT NOT NULL,
                    type TEXT NOT NULL,
                    mint_address TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    weight REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.commit()

        signal = build_signal(
            mint="mint-db",
            source=SignalSourceEnum.PUMP_FUN,
            confidence=0.8,
            observed_at=BASE_TIME,
        )
        aggregator = SignalAggregator([FakeSource("a", [signal])], db=db_path)

        ranked = await aggregator.poll_all()

        assert len(ranked) == 1
        with sqlite3.connect(db_path) as connection:
            row = connection.execute(
                "SELECT mint_address, confidence, payload_json FROM signals"
            ).fetchone()
        assert row is not None
        assert row[0] == "mint-db"
        assert row[1] == pytest.approx(0.8)
        assert '"mint_address":"mint-db"' in row[2]

    asyncio.run(run())
