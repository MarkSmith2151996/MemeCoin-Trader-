"""Paper-only minimum-evidence gate for launch research diagnostics."""

from __future__ import annotations

from dataclasses import dataclass

from src.core.models import CheckResult, RiskAssessment, Signal
from src.signals.modes import CandidateMode, classify_candidate_mode


PAPER_MINIMUM_PROFILE = "paper_minimum"
PAPER_LAUNCH_RESEARCH_MODE = "paper_launch_research"


@dataclass(frozen=True)
class PaperMinimumEvidenceResult:
    """In-memory paper-only eligibility and its explicit evidence labels."""

    eligible: bool
    reason_labels: tuple[str, ...]


def evaluate_paper_minimum_evidence(
    signal: Signal,
    assessment: RiskAssessment,
    *,
    research_mode: str | None = None,
) -> PaperMinimumEvidenceResult:
    """Evaluate paper research eligibility without changing strict/live risk results."""

    blockers = _required_evidence_blockers(assessment)
    blockers.extend(_known_risk_blockers(assessment))
    if blockers:
        return PaperMinimumEvidenceResult(eligible=False, reason_labels=tuple(blockers))

    deferred: list[str] = []
    if assessment.honeypot_check == CheckResult.UNKNOWN:
        deferred.append("paper_minimum_deferred_honeypot_unknown")
    if assessment.creator_holding_check == CheckResult.UNKNOWN:
        deferred.append("paper_minimum_deferred_creator_unknown")
    if assessment.unique_buyers_check == CheckResult.UNKNOWN:
        deferred.append("paper_minimum_deferred_unique_buyers_unknown")

    if assessment.age_check == CheckResult.FAIL:
        if research_mode != PAPER_LAUNCH_RESEARCH_MODE or classify_candidate_mode(signal) != CandidateMode.LAUNCH:
            return PaperMinimumEvidenceResult(
                eligible=False,
                reason_labels=("paper_minimum_blocked_age",),
            )
        deferred.append("paper_minimum_deferred_age_launch_research")
    elif assessment.age_check == CheckResult.UNKNOWN:
        return PaperMinimumEvidenceResult(
            eligible=False,
            reason_labels=("paper_minimum_blocked_age_unknown",),
        )

    return PaperMinimumEvidenceResult(
        eligible=True,
        reason_labels=("paper_minimum_pass", *deferred),
    )


def _required_evidence_blockers(assessment: RiskAssessment) -> list[str]:
    required_checks = (
        (assessment.mint_authority_check, "paper_minimum_blocked_authority"),
        (assessment.freeze_authority_check, "paper_minimum_blocked_freeze"),
        (assessment.liquidity_check, "paper_minimum_blocked_liquidity"),
        (assessment.top10_holder_check, "paper_minimum_blocked_top_holders"),
    )
    return [label for result, label in required_checks if result != CheckResult.PASS]


def _known_risk_blockers(assessment: RiskAssessment) -> list[str]:
    blockers: list[str] = []
    if assessment.honeypot_check == CheckResult.FAIL:
        blockers.append("paper_minimum_blocked_honeypot")
    if assessment.creator_holding_check == CheckResult.FAIL:
        blockers.append("paper_minimum_blocked_creator")
    if assessment.unique_buyers_check == CheckResult.FAIL:
        blockers.append("paper_minimum_blocked_unique_buyers")
    return blockers
