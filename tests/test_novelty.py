from datetime import UTC, datetime

import pytest

from src.core.models import Signal, SignalSource, SignalType
from src.signals.novelty import SignalOrigin, summarize_novelty


BASE_TIME = datetime(2026, 7, 12, tzinfo=UTC)


def _signal(
    mint: str,
    source: SignalSource,
    *,
    payload: dict[str, object] | None = None,
) -> Signal:
    return Signal(
        source=source,
        type=SignalType.BUY,
        mint_address=mint,
        observed_at=BASE_TIME,
        payload=payload or {},
    )


@pytest.fixture
def raw_signals() -> list[Signal]:
    return [
        _signal(
            "mint-a",
            SignalSource.WHALE_TRACKER,
            payload={"tracked_wallet": {"label": "alpha"}},
        ),
        _signal("mint-a", SignalSource.TWITTER),
        _signal("mint-b", SignalSource.ONCHAIN),
        _signal(
            "mint-a",
            SignalSource.WHALE_TRACKER,
            payload={"tracked_wallet": {"label": "beta"}},
        ),
    ]


def test_summarize_novelty_counts_raw_unique_novel_and_duplicate_signals(
    raw_signals: list[Signal],
) -> None:
    summary = summarize_novelty(raw_signals)

    assert summary.total_signals == 4
    assert summary.unique_mints == 2
    assert summary.novel_signals == 2
    assert summary.duplicate_signals == 2
    assert summary.invalid_mint_signals == 0
    assert summary.invalid_wallet_origin_signals == 0

    assert summary.source_mix["WHALE_TRACKER"].total_signals == 2
    assert summary.source_mix["WHALE_TRACKER"].unique_mints == 1
    assert summary.source_mix["WHALE_TRACKER"].novel_signals == 1
    assert summary.source_mix["WHALE_TRACKER"].duplicate_signals == 1
    assert summary.source_mix["TWITTER"].novel_signals == 0
    assert summary.source_mix["TWITTER"].duplicate_signals == 1
    assert summary.source_mix["ONCHAIN"].novel_signals == 1


def test_summarize_novelty_keeps_unique_source_and_wallet_origins(
    raw_signals: list[Signal],
) -> None:
    summary = summarize_novelty(raw_signals)

    assert summary.origins_by_mint["mint-a"] == (
        SignalOrigin(source="WHALE_TRACKER", wallet_label="alpha"),
        SignalOrigin(source="TWITTER"),
        SignalOrigin(source="WHALE_TRACKER", wallet_label="beta"),
    )
    assert summary.origins_by_mint["mint-b"] == (SignalOrigin(source="ONCHAIN"),)


def test_summarize_novelty_counts_invalid_mints_and_whale_origins_without_filtering() -> None:
    signals = [
        _signal(
            "  ",
            SignalSource.WHALE_TRACKER,
            payload={"tracked_wallet": {"label": " "}},
        ),
        _signal(
            "mint-c",
            SignalSource.WHALE_TRACKER,
            payload={"tracked_wallet": "malformed"},
        ),
        _signal("mint-c", SignalSource.PUMP_FUN),
    ]

    summary = summarize_novelty(signals)

    assert summary.total_signals == 3
    assert summary.unique_mints == 1
    assert summary.novel_signals == 1
    assert summary.duplicate_signals == 1
    assert summary.invalid_mint_signals == 1
    assert summary.invalid_wallet_origin_signals == 2
    assert summary.source_mix["WHALE_TRACKER"].invalid_mint_signals == 1
    assert summary.source_mix["WHALE_TRACKER"].invalid_wallet_origin_signals == 2
    assert summary.origins_by_mint["mint-c"] == (
        SignalOrigin(source="WHALE_TRACKER"),
        SignalOrigin(source="PUMP_FUN"),
    )


def test_summarize_novelty_does_not_mutate_caller_supplied_signals(raw_signals: list[Signal]) -> None:
    before = [signal.model_dump(mode="json") for signal in raw_signals]

    summarize_novelty(raw_signals)

    assert [signal.model_dump(mode="json") for signal in raw_signals] == before
