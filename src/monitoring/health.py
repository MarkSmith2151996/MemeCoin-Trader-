"""Lightweight health checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class HealthStatus:
    ok: bool
    message: str
    checked_at: datetime


@dataclass(slots=True)
class HealthMonitor:
    """Compatibility wrapper for dashboard health snapshots."""

    max_staleness_s: int = 60

    def status(self) -> dict[str, dict[str, object]]:
        health_status = check_health()
        return {
            "monitoring": {
                "ok": health_status.ok,
                "message": health_status.message,
                "checked_at": health_status.checked_at,
            }
        }

    def stale_components(self) -> list[str]:
        return []


def check_health() -> HealthStatus:
    return HealthStatus(ok=True, message="memecoin-trader scaffold healthy", checked_at=datetime.now(UTC))
