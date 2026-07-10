from datetime import UTC, datetime, timedelta

from src.execution.live_circuit_breaker import LiveCircuitBreaker


def test_below_threshold_does_not_trip_breaker() -> None:
    breaker = LiveCircuitBreaker(rpc_failure_threshold=3)
    breaker.record_health_check(True, observed_at=datetime.now(UTC))
    breaker.record_rpc_failure()
    breaker.record_rpc_failure()

    decision = breaker.status(observed_at=datetime.now(UTC))

    assert decision.allowed is True
    assert decision.diagnostics == ("live_circuit_breaker_clear",)


def test_threshold_trips_breaker() -> None:
    breaker = LiveCircuitBreaker(rpc_failure_threshold=2)
    breaker.record_health_check(True, observed_at=datetime.now(UTC))
    breaker.record_rpc_failure()
    breaker.record_rpc_failure()

    decision = breaker.status(observed_at=datetime.now(UTC))

    assert decision.allowed is False
    assert "rpc_failure_threshold_reached" in decision.diagnostics


def test_stale_or_missing_health_check_trips_breaker() -> None:
    breaker = LiveCircuitBreaker(health_check_max_age_seconds=60)
    missing = breaker.status(observed_at=datetime.now(UTC))
    breaker.record_health_check(True, observed_at=datetime.now(UTC) - timedelta(seconds=120))
    stale = breaker.status(observed_at=datetime.now(UTC))

    assert missing.allowed is False
    assert missing.diagnostics == ("required_health_check_missing",)
    assert stale.allowed is False
    assert "required_health_check_stale" in stale.diagnostics


def test_reset_clears_tripped_breaker_state() -> None:
    breaker = LiveCircuitBreaker(simulation_failure_threshold=1)
    breaker.record_health_check(True, observed_at=datetime.now(UTC))
    breaker.record_simulation_failure()

    assert breaker.status(observed_at=datetime.now(UTC)).allowed is False

    breaker.reset()
    breaker.record_health_check(True, observed_at=datetime.now(UTC))

    assert breaker.status(observed_at=datetime.now(UTC)).allowed is True


def test_paper_mode_is_unaffected() -> None:
    breaker = LiveCircuitBreaker(rpc_failure_threshold=1)
    breaker.record_rpc_failure()

    decision = breaker.status(execution_mode="paper", observed_at=datetime.now(UTC))

    assert decision.allowed is True
    assert decision.diagnostics == ("paper_mode_unaffected",)
