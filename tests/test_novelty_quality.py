import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.core.models import Signal, SignalSource, SignalType
from src.signals.novelty_quality import (
    FixtureCheckRequest,
    FixtureQualityState,
    check_json_signal_fixtures,
)


WINDOW_START = datetime(2026, 7, 12, 12, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=10)


def _record(
    mint: str,
    source: SignalSource,
    *,
    observed_at: datetime = WINDOW_START,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return Signal(
        source=source,
        type=SignalType.BUY,
        mint_address=mint,
        observed_at=observed_at,
        payload=payload or {},
    ).model_dump(mode="json")


def _write_fixture(tmp_path: Path, name: str, payload: object) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _request(
    label: str,
    path: Path,
    *,
    window_end: datetime = WINDOW_END,
) -> FixtureCheckRequest:
    return FixtureCheckRequest(label, path, WINDOW_START, window_end)


def test_checker_reports_complete_for_matched_complete_fixtures(
    tmp_path: Path,
) -> None:
    sample_path = _write_fixture(
        tmp_path,
        "sample.json",
        [_record("mint-a", SignalSource.TWITTER)],
    )
    candidate_path = _write_fixture(
        tmp_path,
        "candidate.json",
        [
            _record(
                "mint-b",
                SignalSource.WHALE_TRACKER,
                payload={"tracked_wallet": {"label": "candidate"}},
            )
        ],
    )
    sample_before = sample_path.read_bytes()
    candidate_before = candidate_path.read_bytes()

    report = check_json_signal_fixtures(
        [_request("sample", sample_path), _request("candidate", candidate_path)]
    )

    assert report.state == FixtureQualityState.COMPLETE
    assert [check.state for check in report.checks] == [
        FixtureQualityState.COMPLETE,
        FixtureQualityState.COMPLETE,
    ]
    assert report.checks[1].summary is not None
    candidate_summary = report.checks[1].summary
    assert candidate_summary.origins_by_mint["mint-b"][0].wallet_label == "candidate"
    assert sample_path.read_bytes() == sample_before
    assert candidate_path.read_bytes() == candidate_before


def test_checker_reports_incomplete_for_missing_explicit_timestamp(
    tmp_path: Path,
) -> None:
    record = _record("mint-a", SignalSource.TWITTER)
    record.pop("observed_at")
    incomplete_path = _write_fixture(tmp_path, "incomplete.json", [record])
    complete_path = _write_fixture(
        tmp_path,
        "complete.json",
        [_record("mint-b", SignalSource.ONCHAIN)],
    )

    report = check_json_signal_fixtures(
        [
            _request("incomplete", incomplete_path),
            _request("complete", complete_path),
        ]
    )

    assert report.state == FixtureQualityState.INCOMPLETE
    assert report.checks[0].state == FixtureQualityState.INCOMPLETE
    assert report.checks[0].reasons == ("record_0_missing_observed_at",)


def test_checker_reports_malformed_for_invalid_json(tmp_path: Path) -> None:
    malformed_path = tmp_path / "malformed.json"
    malformed_path.write_text("{not json", encoding="utf-8")
    complete_path = _write_fixture(
        tmp_path,
        "complete.json",
        [_record("mint-b", SignalSource.ONCHAIN)],
    )

    report = check_json_signal_fixtures(
        [
            _request("malformed", malformed_path),
            _request("complete", complete_path),
        ]
    )

    assert report.state == FixtureQualityState.MALFORMED
    assert report.checks[0].reasons == ("unreadable_or_invalid_json",)


def test_checker_reports_malformed_for_non_object_record(tmp_path: Path) -> None:
    malformed_path = _write_fixture(tmp_path, "malformed.json", ["not-a-record"])
    complete_path = _write_fixture(
        tmp_path,
        "complete.json",
        [_record("mint-b", SignalSource.ONCHAIN)],
    )

    report = check_json_signal_fixtures(
        [
            _request("malformed", malformed_path),
            _request("complete", complete_path),
        ]
    )

    assert report.state == FixtureQualityState.MALFORMED
    assert report.checks[0].reasons == ("record_0_not_object",)


def test_checker_reports_unmatched_window_for_different_declared_windows(
    tmp_path: Path,
) -> None:
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

    report = check_json_signal_fixtures(
        [
            _request("first", first_path),
            _request(
                "second",
                second_path,
                window_end=WINDOW_END + timedelta(minutes=1),
            ),
        ]
    )

    assert report.state == FixtureQualityState.UNMATCHED_WINDOW
    assert all(
        check.state == FixtureQualityState.UNMATCHED_WINDOW for check in report.checks
    )
    assert all("window_mismatch" in check.reasons for check in report.checks)


def test_checker_reports_inconclusive_without_valid_novel_mints(tmp_path: Path) -> None:
    first_path = _write_fixture(tmp_path, "first.json", [])
    second_path = _write_fixture(tmp_path, "second.json", [])

    report = check_json_signal_fixtures(
        [_request("first", first_path), _request("second", second_path)]
    )

    assert report.state == FixtureQualityState.INCONCLUSIVE
    assert all(check.state == FixtureQualityState.INCONCLUSIVE for check in report.checks)
