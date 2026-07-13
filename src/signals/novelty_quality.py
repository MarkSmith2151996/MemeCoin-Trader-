"""Fixture-only quality checks for manual novelty comparisons."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from pydantic import ValidationError

from src.core.models import Signal
from src.signals.novelty import NoveltySummary, summarize_novelty


class FixtureQualityState(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    MALFORMED = "malformed"
    UNMATCHED_WINDOW = "unmatched_window"
    INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True)
class FixtureCheckRequest:
    """One explicit fixture path and its manually recorded collection window."""

    label: str
    path: str | Path
    window_start: datetime
    window_end: datetime


@dataclass(frozen=True)
class FixtureQualityCheck:
    """Field, window, and novelty state for one caller-supplied fixture."""

    label: str
    state: FixtureQualityState
    reasons: tuple[str, ...]
    summary: NoveltySummary | None


@dataclass(frozen=True)
class FixtureQualityReport:
    """Aggregate quality state for a caller-supplied matched fixture set."""

    state: FixtureQualityState
    reasons: tuple[str, ...]
    checks: tuple[FixtureQualityCheck, ...]


def check_json_signal_fixtures(
    requests: Iterable[FixtureCheckRequest],
) -> FixtureQualityReport:
    """Validate explicit JSON fixtures and their declared matched-window metadata."""

    requests = tuple(requests)
    checks = tuple(_check_fixture(request) for request in requests)
    if len(checks) < 2:
        return FixtureQualityReport(
            state=FixtureQualityState.INCONCLUSIVE,
            reasons=("fewer_than_two_fixtures",),
            checks=checks,
        )

    if any(check.state == FixtureQualityState.MALFORMED for check in checks):
        return FixtureQualityReport(
            state=FixtureQualityState.MALFORMED,
            reasons=("malformed_fixture",),
            checks=checks,
        )
    if any(check.state == FixtureQualityState.INCOMPLETE for check in checks):
        return FixtureQualityReport(
            state=FixtureQualityState.INCOMPLETE,
            reasons=("incomplete_fixture",),
            checks=checks,
        )

    declared_windows = {
        (request.window_start, request.window_end)
        for request in requests
    }
    windows_match = len(declared_windows) == 1
    has_unmatched_window = any(
        check.state == FixtureQualityState.UNMATCHED_WINDOW for check in checks
    )
    if not windows_match or has_unmatched_window:
        window_checks = tuple(
            check
            if check.state == FixtureQualityState.UNMATCHED_WINDOW
            else replace(
                check,
                state=FixtureQualityState.UNMATCHED_WINDOW,
                reasons=(*check.reasons, "window_mismatch"),
            )
            for check in checks
        )
        return FixtureQualityReport(
            state=FixtureQualityState.UNMATCHED_WINDOW,
            reasons=("window_mismatch",),
            checks=window_checks,
        )
    if any(check.state == FixtureQualityState.INCONCLUSIVE for check in checks):
        return FixtureQualityReport(
            state=FixtureQualityState.INCONCLUSIVE,
            reasons=("no_valid_novel_mints",),
            checks=checks,
        )
    return FixtureQualityReport(
        state=FixtureQualityState.COMPLETE,
        reasons=(),
        checks=checks,
    )


def _check_fixture(request: FixtureCheckRequest) -> FixtureQualityCheck:
    label = request.label.strip()
    if not label:
        return FixtureQualityCheck(
            label=request.label,
            state=FixtureQualityState.INCOMPLETE,
            reasons=("blank_label",),
            summary=None,
        )
    if request.window_start > request.window_end:
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.INCOMPLETE,
            reasons=("invalid_window",),
            summary=None,
        )

    try:
        payload = json.loads(Path(request.path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.MALFORMED,
            reasons=("unreadable_or_invalid_json",),
            summary=None,
        )
    if not isinstance(payload, list):
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.MALFORMED,
            reasons=("top_level_not_array",),
            summary=None,
        )

    non_object_reasons: list[str] = []
    raw_reasons: list[str] = []
    records: list[dict[str, object]] = []
    for index, record in enumerate(payload):
        if not isinstance(record, dict):
            non_object_reasons.append(f"record_{index}_not_object")
            continue
        records.append(record)
        raw_reasons.extend(_missing_field_reasons(index, record))
    if non_object_reasons:
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.MALFORMED,
            reasons=tuple(non_object_reasons),
            summary=None,
        )
    if raw_reasons:
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.INCOMPLETE,
            reasons=tuple(raw_reasons),
            summary=None,
        )

    try:
        signals = [Signal.model_validate(record) for record in records]
    except ValidationError:
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.MALFORMED,
            reasons=("invalid_signal_record",),
            summary=None,
        )

    if any(
        not request.window_start <= signal.observed_at <= request.window_end
        for signal in signals
    ):
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.UNMATCHED_WINDOW,
            reasons=("record_outside_declared_window",),
            summary=summarize_novelty(signals),
        )

    summary = summarize_novelty(signals)
    if summary.novel_signals == 0:
        return FixtureQualityCheck(
            label=label,
            state=FixtureQualityState.INCONCLUSIVE,
            reasons=("no_valid_novel_mints",),
            summary=summary,
        )
    return FixtureQualityCheck(
        label=label,
        state=FixtureQualityState.COMPLETE,
        reasons=(),
        summary=summary,
    )


def _missing_field_reasons(index: int, record: dict[str, object]) -> list[str]:
    required_fields = ("source", "type", "mint_address", "observed_at")
    reasons = [
        f"record_{index}_missing_{field}"
        for field in required_fields
        if not isinstance(record.get(field), str) or not record[field].strip()
    ]
    if record.get("source") != "WHALE_TRACKER":
        return reasons

    payload = record.get("payload")
    tracked_wallet = payload.get("tracked_wallet") if isinstance(payload, dict) else None
    label = tracked_wallet.get("label") if isinstance(tracked_wallet, dict) else None
    if not isinstance(label, str) or not label.strip():
        reasons.append(f"record_{index}_missing_whale_label")
    return reasons
