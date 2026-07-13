"""Manual comparison of explicit JSON signal-fixture novelty summaries."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from src.signals.novelty import NoveltySummary
from src.signals.novelty_fixture import summarize_json_signal_fixture


@dataclass(frozen=True)
class FixtureNoveltyComparison:
    """One labeled fixture's novelty summary and valid-observation duplicate rate."""

    label: str
    summary: NoveltySummary
    duplicate_rate: float | None


def compare_json_signal_fixtures(
    fixtures: Mapping[str, str | Path],
) -> tuple[FixtureNoveltyComparison, ...]:
    """Compare at least two labeled caller-supplied JSON signal fixtures.

    Mapping order is preserved for manual matched-window comparison. A duplicate
    rate is reported only when the fixture has at least one valid mint observation.
    """

    if len(fixtures) < 2:
        raise ValueError("At least two labeled signal fixtures are required for comparison.")

    comparisons: list[FixtureNoveltyComparison] = []
    for label, path in fixtures.items():
        normalized_label = label.strip()
        if not normalized_label:
            raise ValueError("Signal fixture labels must be nonblank.")

        summary = summarize_json_signal_fixture(path)
        valid_observations = summary.novel_signals + summary.duplicate_signals
        duplicate_rate = (
            summary.duplicate_signals / valid_observations if valid_observations else None
        )
        comparisons.append(
            FixtureNoveltyComparison(
                label=normalized_label,
                summary=summary,
                duplicate_rate=duplicate_rate,
            )
        )
    return tuple(comparisons)
