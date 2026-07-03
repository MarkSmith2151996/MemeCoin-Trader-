"""Lightweight health checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class HealthStatus:
    ok: bool
    message: str
    checked_at: datetime


def check_health() -> HealthStatus:
    return HealthStatus(ok=True, message="memecoin-trader scaffold healthy", checked_at=datetime.now(UTC))
