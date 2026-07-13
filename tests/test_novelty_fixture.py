import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.core.models import Signal, SignalSource, SignalType
from src.signals.novelty_fixture import summarize_json_signal_fixture


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


def _write_fixture(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "signals.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_summarize_json_signal_fixture_reads_caller_supplied_records(tmp_path: Path) -> None:
    fixture_path = _write_fixture(
        tmp_path,
        [
            _record(
                "mint-a",
                SignalSource.WHALE_TRACKER,
                payload={"tracked_wallet": {"label": "alpha"}},
            ),
            _record("mint-a", SignalSource.TWITTER),
            _record("mint-b", SignalSource.ONCHAIN),
        ],
    )
    before = fixture_path.read_bytes()

    summary = summarize_json_signal_fixture(fixture_path)

    assert summary.total_signals == 3
    assert summary.unique_mints == 2
    assert summary.novel_signals == 2
    assert summary.duplicate_signals == 1
    assert summary.source_mix["WHALE_TRACKER"].novel_signals == 1
    assert summary.source_mix["TWITTER"].duplicate_signals == 1
    assert summary.origins_by_mint["mint-a"][0].wallet_label == "alpha"
    assert fixture_path.read_bytes() == before


def test_summarize_json_signal_fixture_rejects_non_array_payload(tmp_path: Path) -> None:
    fixture_path = _write_fixture(tmp_path, {"signals": []})

    with pytest.raises(ValueError, match="top-level JSON array"):
        summarize_json_signal_fixture(fixture_path)


def test_summarize_json_signal_fixture_rejects_non_object_record(tmp_path: Path) -> None:
    fixture_path = _write_fixture(tmp_path, ["not-a-signal"])

    with pytest.raises(ValueError, match="record at index 0 must be an object"):
        summarize_json_signal_fixture(fixture_path)
