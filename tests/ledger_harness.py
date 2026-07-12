"""In-memory synthetic adapter used only to exercise disabled ledger contracts."""

from __future__ import annotations

from dataclasses import dataclass, field

from src.core.decision_ledger import (
    LedgerDecisionEvidence,
    LedgerDecisionLookup,
    LedgerDecisionSearch,
    LedgerProviderObservation,
)


@dataclass
class SyntheticLedgerAdapter:
    """Test-only, process-local adapter with no database or file persistence."""

    decisions: dict[str, LedgerDecisionEvidence] = field(default_factory=dict)
    provider_observations: dict[str, LedgerProviderObservation] = field(default_factory=dict)

    def record_decision(self, evidence: LedgerDecisionEvidence) -> None:
        """Store one synthetic immutable decision record for contract testing."""

        if evidence.decision_id in self.decisions:
            raise ValueError("Synthetic decision IDs must be unique.")
        self.decisions[evidence.decision_id] = evidence.model_copy(deep=True)

    def record_provider_observation(self, observation: LedgerProviderObservation) -> None:
        """Store one synthetic redacted observation for contract testing."""

        if observation.snapshot_id in self.provider_observations:
            raise ValueError("Synthetic snapshot IDs must be unique.")
        self.provider_observations[observation.snapshot_id] = observation.model_copy(deep=True)

    def read_decision(self, lookup: LedgerDecisionLookup) -> LedgerDecisionEvidence | None:
        """Return one synthetic decision, if it was recorded in this instance."""

        return self.decisions.get(lookup.decision_id)

    def search_decisions(self, search: LedgerDecisionSearch) -> list[LedgerDecisionEvidence]:
        """Apply only the bounded planner-facing filters to synthetic decisions."""

        matches = [
            decision
            for decision in self.decisions.values()
            if (search.mint_address is None or decision.mint_address == search.mint_address)
            and (search.source is None or decision.source == search.source)
            and (search.outcome_status is None or decision.outcome_status == search.outcome_status)
        ]
        return matches[: search.limit]
