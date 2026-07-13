import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.core.models import Signal, SignalSource, SignalType
from src.signals.novelty_compare import compare_json_signal_fixtures


BASE_TIME = datetime(2026, 7, 12, tzinfo=UTC)


def _record(
    mint: str,
    source: SignalSource,
    *,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return Signal(
        source=source,
        type=SignalType.BUY,
        mint_address=mint,
        observed_at=BASE_TIME,
        payload=payload or {},
    ).model_dump(mode="json")


def _write_fixture(
    tmp_path: Path,
    name: str,
    records: list[dict[str, object]],
) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def test_compare_json_signal_fixtures_returns_ordered_matched_summaries(
    tmp_path: Path,
) -> None:
    sample_path = _write_fixture(
        tmp_path,
        "sample.json",
        [
            _record(
                "mint-a",
                SignalSource.WHALE_TRACKER,
                payload={"tracked_wallet": {"label": "sample"}},
            ),
            _record("mint-a", SignalSource.TWITTER),
            _record("mint-b", SignalSource.ONCHAIN),
        ],
    )
    candidate_path = _write_fixture(
        tmp_path,
        "candidate.json",
        [
            _record(
                "mint-c",
                SignalSource.WHALE_TRACKER,
                payload={"tracked_wallet": {"label": "candidate"}},
            ),
            _record("mint-d", SignalSource.TWITTER),
            _record("mint-e", SignalSource.ONCHAIN),
        ],
    )
    sample_before = sample_path.read_bytes()
    candidate_before = candidate_path.read_bytes()

    comparisons = compare_json_signal_fixtures(
        {"sample wallet": sample_path, "candidate wallet": candidate_path}
    )

    sample, candidate = comparisons
    assert [comparison.label for comparison in comparisons] == [
        "sample wallet",
        "candidate wallet",
    ]
    assert sample.summary.total_signals == 3
    assert sample.summary.unique_mints == 2
    assert sample.summary.novel_signals == 2
    assert sample.summary.duplicate_signals == 1
    assert sample.duplicate_rate == pytest.approx(1 / 3)
    assert sample.summary.source_mix["WHALE_TRACKER"].novel_signals == 1
    assert sample.summary.origins_by_mint["mint-a"][0].wallet_label == "sample"
    assert candidate.summary.unique_mints == 3
    assert candidate.duplicate_rate == 0.0
    assert candidate.summary.origins_by_mint["mint-c"][0].wallet_label == "candidate"
    assert sample_path.read_bytes() == sample_before
    assert candidate_path.read_bytes() == candidate_before


def test_compare_json_signal_fixtures_requires_multiple_labeled_fixtures(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        "only.json",
        [_record("mint-a", SignalSource.TWITTER)],
    )

    with pytest.raises(ValueError, match="At least two"):
        compare_json_signal_fixtures({"only": fixture_path})


def test_compare_json_signal_fixtures_rejects_blank_labels(tmp_path: Path) -> None:
    first_path = _write_fixture(
        tmp_path,
        "first.json",
        [_record("mint-a", SignalSource.TWITTER)],
    )
    second_path = _write_fixture(
        tmp_path,
        "second.json",
        [_record("mint-b", SignalSource.ONCHAIN)],
    )

    with pytest.raises(ValueError, match="labels must be nonblank"):
        compare_json_signal_fixtures({" ": first_path, "second": second_path})
