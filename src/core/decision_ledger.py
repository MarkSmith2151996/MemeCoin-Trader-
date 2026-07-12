"""Disabled diagnostic boundary for a future PostgreSQL decision ledger.

This module deliberately has no database client, SQL execution, or runtime caller.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from src.core.models import CheckResult


class LedgerDecisionEvidence(BaseModel):
    """Allowlisted evidence shape for a future narrow decision-write tool."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str
    mint_address: str
    checks: dict[str, CheckResult] = Field(default_factory=dict)
    provider_status: str = "unknown"


class LedgerPrototypeDisabledError(RuntimeError):
    """Raised because the prototype must not write during runtime operation."""


def write_memecoin_decision(evidence: LedgerDecisionEvidence) -> None:
    """Reserve the future narrow writer name without enabling any persistence."""

    del evidence
    raise LedgerPrototypeDisabledError("Decision ledger writes are disabled in this diagnostic prototype.")
