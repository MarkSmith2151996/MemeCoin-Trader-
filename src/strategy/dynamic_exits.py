"""Pure helper logic for future dynamic exit calibration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta


DEFAULT_VOLUME_DECAY_RATIO = 0.20
DEFAULT_VOLUME_DECAY_DURATION = timedelta(minutes=15)
DEFAULT_TRAIL_START_MULTIPLE = 3.0
DEFAULT_LIQUIDITY_DROP_RATIO = 0.50
DEFAULT_LIQUIDITY_DROP_WINDOW = timedelta(seconds=60)


@dataclass(frozen=True)
class DynamicExitResult:
    triggered: bool
    reason_labels: tuple[str, ...] = ()
    details: dict[str, float | int | str | None] = field(default_factory=dict)


@dataclass(frozen=True)
class DynamicExitSummary:
    volume_decay: DynamicExitResult
    trail_start: DynamicExitResult
    liquidity_emergency: DynamicExitResult

    @property
    def reason_labels(self) -> tuple[str, ...]:
        labels: list[str] = []
        for result in (self.volume_decay, self.trail_start, self.liquidity_emergency):
            labels.extend(result.reason_labels)
        return tuple(labels)


def update_peak_volume(peak_volume: float | None, current_volume: float) -> float:
    normalized_peak = max(peak_volume or 0.0, 0.0)
    return max(normalized_peak, max(current_volume, 0.0))


def evaluate_volume_decay(
    *,
    current_volume: float,
    peak_volume: float,
    below_threshold_started_at: datetime | None,
    observed_at: datetime,
    threshold_ratio: float = DEFAULT_VOLUME_DECAY_RATIO,
    min_duration: timedelta = DEFAULT_VOLUME_DECAY_DURATION,
) -> DynamicExitResult:
    if current_volume < 0 or peak_volume <= 0:
        return DynamicExitResult(
            triggered=False,
            details={
                "current_volume": max(current_volume, 0.0),
                "peak_volume": max(peak_volume, 0.0),
                "volume_ratio": 0.0,
                "minutes_below_threshold": 0.0,
            },
        )

    volume_ratio = current_volume / peak_volume
    if volume_ratio >= threshold_ratio or below_threshold_started_at is None:
        minutes_below_threshold = 0.0
    else:
        minutes_below_threshold = max((observed_at - below_threshold_started_at).total_seconds() / 60, 0.0)

    triggered = (
        volume_ratio < threshold_ratio
        and below_threshold_started_at is not None
        and observed_at - below_threshold_started_at >= min_duration
    )
    reason_labels = ("volume_decay_exit",) if triggered else ()
    return DynamicExitResult(
        triggered=triggered,
        reason_labels=reason_labels,
        details={
            "current_volume": current_volume,
            "peak_volume": peak_volume,
            "volume_ratio": volume_ratio,
            "minutes_below_threshold": minutes_below_threshold,
        },
    )


def evaluate_trail_start(
    *,
    current_multiple: float,
    trail_start_multiple: float = DEFAULT_TRAIL_START_MULTIPLE,
) -> DynamicExitResult:
    triggered = current_multiple >= trail_start_multiple
    reason_labels = ("trail_start_ready",) if triggered else ()
    return DynamicExitResult(
        triggered=triggered,
        reason_labels=reason_labels,
        details={
            "current_multiple": current_multiple,
            "trail_start_multiple": trail_start_multiple,
        },
    )


def evaluate_liquidity_emergency(
    *,
    current_liquidity: float,
    reference_liquidity: float,
    reference_at: datetime,
    observed_at: datetime,
    drop_ratio_threshold: float = DEFAULT_LIQUIDITY_DROP_RATIO,
    max_window: timedelta = DEFAULT_LIQUIDITY_DROP_WINDOW,
) -> DynamicExitResult:
    if current_liquidity < 0 or reference_liquidity <= 0:
        return DynamicExitResult(
            triggered=False,
            details={
                "current_liquidity": max(current_liquidity, 0.0),
                "reference_liquidity": max(reference_liquidity, 0.0),
                "drop_ratio": 0.0,
                "elapsed_seconds": 0.0,
            },
        )

    elapsed = max((observed_at - reference_at).total_seconds(), 0.0)
    drop_ratio = max((reference_liquidity - current_liquidity) / reference_liquidity, 0.0)
    triggered = elapsed <= max_window.total_seconds() and drop_ratio >= drop_ratio_threshold
    reason_labels = ("liquidity_emergency_exit",) if triggered else ()
    return DynamicExitResult(
        triggered=triggered,
        reason_labels=reason_labels,
        details={
            "current_liquidity": current_liquidity,
            "reference_liquidity": reference_liquidity,
            "drop_ratio": drop_ratio,
            "elapsed_seconds": elapsed,
        },
    )


def summarize_dynamic_exit_checks(
    *,
    volume_decay: DynamicExitResult,
    trail_start: DynamicExitResult,
    liquidity_emergency: DynamicExitResult,
) -> DynamicExitSummary:
    return DynamicExitSummary(
        volume_decay=volume_decay,
        trail_start=trail_start,
        liquidity_emergency=liquidity_emergency,
    )
