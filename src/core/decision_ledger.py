"""Disabled diagnostic boundary for a future PostgreSQL decision ledger.

This module deliberately has no database client, SQL execution, or runtime caller.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.core.models import CheckResult


class LedgerDecisionEvidence(BaseModel):
    """Allowlisted request for a future diagnostic decision-recording tool."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str
    mint_address: str
    source: str = "unknown"
    mode: Literal["paper", "live"] = "paper"
    decision_type: Literal["seen", "rejected", "accepted", "entry", "exit", "labeled"] = "seen"
    outcome_status: str = "unknown"
    checks: dict[str, CheckResult] = Field(default_factory=dict)
    provider_status: str = "unknown"


class LedgerProviderObservation(BaseModel):
    """Allowlisted request for a future redacted provider-observation tool."""

    model_config = ConfigDict(extra="forbid")

    snapshot_id: str
    mint_address: str
    provider_name: str
    provider_status: str
    observed_at: str
    field_presence: list[str] = Field(default_factory=list)
    normalized_data: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    unavailable_reason: str | None = None
    source_decision_id: str | None = None


class LedgerHistoricalProvenance(BaseModel):
    """Preserved origin for one explicitly exported SQLite decision record."""

    model_config = ConfigDict(extra="forbid")

    source_system: Literal["sqlite"]
    source_table: Literal["paper_decisions"]
    source_record_id: str
    source_observed_at: str
    extraction_method: Literal["explicit_export"]


class LedgerHistoricalImport(BaseModel):
    """Disabled import contract for historical diagnostic evidence only."""

    model_config = ConfigDict(extra="forbid")

    import_id: str
    provenance: LedgerHistoricalProvenance
    decision: LedgerDecisionEvidence
    provider_observations: list[LedgerProviderObservation] = Field(default_factory=list)
    outcome_status: Literal["unknown", "inconclusive"] = "unknown"
    outcome_claim: Literal["not_claimed"] = "not_claimed"

    @model_validator(mode="after")
    def preserves_decision_evidence_links(self) -> "LedgerHistoricalImport":
        for observation in self.provider_observations:
            if observation.source_decision_id != self.decision.decision_id:
                raise ValueError("Each provider observation must reference the imported decision ID.")
        return self


class LedgerDecisionLookup(BaseModel):
    """Bounded request for one diagnostic decision by its immutable ID."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str


class LedgerDecisionSearch(BaseModel):
    """Bounded read-only decision search by mint, source, or outcome status."""

    model_config = ConfigDict(extra="forbid")

    mint_address: str | None = None
    source: str | None = None
    outcome_status: str | None = None
    limit: int = Field(default=50, ge=1, le=100)

    @model_validator(mode="after")
    def has_filter(self) -> "LedgerDecisionSearch":
        if not any((self.mint_address, self.source, self.outcome_status)):
            raise ValueError("A mint address, source, or outcome status is required.")
        return self


class LedgerPrototypeDisabledError(RuntimeError):
    """Raised because the prototype must not write during runtime operation."""


def write_memecoin_decision(evidence: LedgerDecisionEvidence) -> None:
    """Reserve the future narrow writer name without enabling any persistence."""

    del evidence
    raise LedgerPrototypeDisabledError("Decision ledger writes are disabled in this diagnostic prototype.")


def record_provider_observation(observation: LedgerProviderObservation) -> None:
    """Reserve a narrow provider-observation writer without enabling persistence."""

    del observation
    raise LedgerPrototypeDisabledError("Decision ledger writes are disabled in this diagnostic prototype.")


def import_historical_ledger_evidence(import_request: LedgerHistoricalImport) -> None:
    """Reserve a provenance-preserving import without enabling persistence."""

    del import_request
    raise LedgerPrototypeDisabledError("Historical ledger imports are disabled in this diagnostic prototype.")


def read_memecoin_decision(lookup: LedgerDecisionLookup) -> None:
    """Reserve a decision-by-ID reader without enabling database access."""

    del lookup
    raise LedgerPrototypeDisabledError("Decision ledger reads are disabled in this diagnostic prototype.")


def search_memecoin_decisions(search: LedgerDecisionSearch) -> None:
    """Reserve a bounded decision search without enabling database access."""

    del search
    raise LedgerPrototypeDisabledError("Decision ledger reads are disabled in this diagnostic prototype.")
