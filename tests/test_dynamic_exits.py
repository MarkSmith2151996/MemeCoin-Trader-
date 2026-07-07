from datetime import UTC, datetime, timedelta

from src.strategy import exits
from src.strategy.dynamic_exits import (
    DEFAULT_TRAIL_START_MULTIPLE,
    evaluate_liquidity_emergency,
    evaluate_trail_start,
    evaluate_volume_decay,
    summarize_dynamic_exit_checks,
)


def test_volume_decay_triggers_at_nineteen_percent_for_fifteen_minutes() -> None:
    observed_at = datetime.now(UTC)
    result = evaluate_volume_decay(
        current_volume=19.0,
        peak_volume=100.0,
        below_threshold_started_at=observed_at - timedelta(minutes=15),
        observed_at=observed_at,
    )

    assert result.triggered is True
    assert result.reason_labels == ("volume_decay_exit",)
    assert result.details["volume_ratio"] == 0.19


def test_volume_decay_does_not_trigger_at_twenty_five_percent() -> None:
    observed_at = datetime.now(UTC)
    result = evaluate_volume_decay(
        current_volume=25.0,
        peak_volume=100.0,
        below_threshold_started_at=observed_at - timedelta(minutes=20),
        observed_at=observed_at,
    )

    assert result.triggered is False
    assert result.reason_labels == ()
    assert result.details["minutes_below_threshold"] == 0.0


def test_liquidity_drop_of_fifty_percent_within_sixty_seconds_triggers_emergency() -> None:
    observed_at = datetime.now(UTC)
    result = evaluate_liquidity_emergency(
        current_liquidity=50.0,
        reference_liquidity=100.0,
        reference_at=observed_at - timedelta(seconds=45),
        observed_at=observed_at,
    )

    assert result.triggered is True
    assert result.reason_labels == ("liquidity_emergency_exit",)
    assert result.details["drop_ratio"] == 0.5


def test_liquidity_drop_of_twenty_percent_does_not_trigger_emergency() -> None:
    observed_at = datetime.now(UTC)
    result = evaluate_liquidity_emergency(
        current_liquidity=80.0,
        reference_liquidity=100.0,
        reference_at=observed_at - timedelta(seconds=45),
        observed_at=observed_at,
    )

    assert result.triggered is False
    assert result.reason_labels == ()
    assert result.details["drop_ratio"] == 0.2


def test_trail_start_helper_respects_three_x_and_four_x_thresholds() -> None:
    default_result = evaluate_trail_start(current_multiple=3.0)
    custom_result = evaluate_trail_start(current_multiple=3.5, trail_start_multiple=4.0)

    assert DEFAULT_TRAIL_START_MULTIPLE == 3.0
    assert default_result.triggered is True
    assert default_result.reason_labels == ("trail_start_ready",)
    assert custom_result.triggered is False
    assert custom_result.details["trail_start_multiple"] == 4.0


def test_importing_dynamic_helpers_does_not_change_runtime_exit_behavior() -> None:
    observed_at = datetime.now(UTC)
    summary = summarize_dynamic_exit_checks(
        volume_decay=evaluate_volume_decay(
            current_volume=19.0,
            peak_volume=100.0,
            below_threshold_started_at=observed_at - timedelta(minutes=16),
            observed_at=observed_at,
        ),
        trail_start=evaluate_trail_start(current_multiple=3.5),
        liquidity_emergency=evaluate_liquidity_emergency(
            current_liquidity=80.0,
            reference_liquidity=100.0,
            reference_at=observed_at - timedelta(seconds=30),
            observed_at=observed_at,
        ),
    )

    assert exits.DEFAULT_TP_LEVELS[0] == (2.0, 0.25)
    assert summary.reason_labels == ("volume_decay_exit", "trail_start_ready")
