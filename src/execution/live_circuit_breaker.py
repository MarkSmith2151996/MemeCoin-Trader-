"""Operational live circuit breaker for infrastructure failures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class LiveCircuitBreakerDecision:
    allowed: bool
    diagnostics: tuple[str, ...]


class LiveCircuitBreaker:
    def __init__(
        self,
        *,
        rpc_failure_threshold: int = 3,
        simulation_failure_threshold: int = 3,
        submission_failure_threshold: int = 3,
        health_check_max_age_seconds: int = 120,
    ) -> None:
        self.rpc_failure_threshold = rpc_failure_threshold
        self.simulation_failure_threshold = simulation_failure_threshold
        self.submission_failure_threshold = submission_failure_threshold
        self.health_check_max_age = timedelta(seconds=health_check_max_age_seconds)
        self._rpc_failures = 0
        self._simulation_failures = 0
        self._submission_failures = 0
        self._last_health_check_at: datetime | None = None
        self._last_health_check_ok: bool | None = None

    def status(self, *, execution_mode: str = "live", observed_at: datetime | None = None) -> LiveCircuitBreakerDecision:
        if execution_mode != "live":
            return LiveCircuitBreakerDecision(allowed=True, diagnostics=("paper_mode_unaffected",))

        now = observed_at or datetime.now(UTC)
        diagnostics: list[str] = []
        if self._rpc_failures >= self.rpc_failure_threshold:
            diagnostics.append("rpc_failure_threshold_reached")
        if self._simulation_failures >= self.simulation_failure_threshold:
            diagnostics.append("simulation_failure_threshold_reached")
        if self._submission_failures >= self.submission_failure_threshold:
            diagnostics.append("submission_failure_threshold_reached")
        if self._last_health_check_at is None:
            diagnostics.append("required_health_check_missing")
        else:
            if self._last_health_check_ok is not True:
                diagnostics.append("required_health_check_failed")
            if now - self._last_health_check_at > self.health_check_max_age:
                diagnostics.append("required_health_check_stale")

        if diagnostics:
            return LiveCircuitBreakerDecision(allowed=False, diagnostics=tuple(diagnostics))
        return LiveCircuitBreakerDecision(allowed=True, diagnostics=("live_circuit_breaker_clear",))

    def record_rpc_failure(self) -> None:
        self._rpc_failures += 1

    def record_rpc_success(self) -> None:
        self._rpc_failures = 0

    def record_simulation_failure(self) -> None:
        self._simulation_failures += 1

    def record_simulation_success(self) -> None:
        self._simulation_failures = 0

    def record_submission_failure(self) -> None:
        self._submission_failures += 1

    def record_submission_success(self) -> None:
        self._submission_failures = 0

    def record_health_check(self, ok: bool, *, observed_at: datetime | None = None) -> None:
        self._last_health_check_ok = ok
        self._last_health_check_at = observed_at or datetime.now(UTC)

    def reset(self) -> None:
        self._rpc_failures = 0
        self._simulation_failures = 0
        self._submission_failures = 0
        self._last_health_check_at = None
        self._last_health_check_ok = None
