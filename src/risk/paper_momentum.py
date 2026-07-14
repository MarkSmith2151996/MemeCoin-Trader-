"""Isolated paper-only evidence gate for New Pairs momentum experiments."""

from __future__ import annotations

import math

from src.core.models import CheckResult, RiskAssessment, Signal
from src.risk.paper_minimum import PAPER_LAUNCH_RESEARCH_MODE, PaperMinimumEvidenceResult
from src.signals.modes import CandidateMode, classify_candidate_mode


PAPER_NEW_PAIRS_MOMENTUM_PROFILE = "paper_new_pairs_momentum"
PAPER_NEW_PAIRS_MOMENTUM_MAX_TOP10_HOLDER_PCT = 90.0


def evaluate_paper_new_pairs_momentum_evidence(
    signal: Signal,
    assessment: RiskAssessment,
    *,
    top10_holder_pct: float | None,
    research_mode: str | None = PAPER_LAUNCH_RESEARCH_MODE,
    ui_age_minutes: float | None = None,
    ui_max_age_minutes: float | None = None,
) -> PaperMinimumEvidenceResult:
    """Evaluate the explicit 90% paper-only lane without altering strict evidence."""

    blockers = _required_evidence_blockers(assessment)
    blockers.extend(_known_risk_blockers(assessment))
    if not _is_accepted_holder_concentration(top10_holder_pct):
        blockers.append("paper_momentum_blocked_top_holders")
    if blockers:
        return PaperMinimumEvidenceResult(eligible=False, reason_labels=tuple(blockers))

    deferred: list[str] = []
    if assessment.honeypot_check == CheckResult.UNKNOWN:
        deferred.append("paper_momentum_deferred_honeypot_unknown")
    if assessment.creator_holding_check == CheckResult.UNKNOWN:
        deferred.append("paper_momentum_deferred_creator_unknown")
    if assessment.unique_buyers_check == CheckResult.UNKNOWN:
        deferred.append("paper_momentum_deferred_unique_buyers_unknown")

    if assessment.age_check == CheckResult.FAIL:
        if research_mode != PAPER_LAUNCH_RESEARCH_MODE or classify_candidate_mode(signal) != CandidateMode.LAUNCH:
            return PaperMinimumEvidenceResult(False, ("paper_momentum_blocked_age",))
        deferred.append("paper_momentum_deferred_age_launch_research")
    elif assessment.age_check == CheckResult.UNKNOWN:
        if not _is_ui_age_observed_fresh(ui_age_minutes, ui_max_age_minutes):
            return PaperMinimumEvidenceResult(False, ("paper_momentum_blocked_age_unknown",))
        deferred.append("paper_momentum_age_ui_observed_fresh")

    return PaperMinimumEvidenceResult(True, ("paper_momentum_pass", *deferred))


def _is_accepted_holder_concentration(value: float | None) -> bool:
    return (
        value is not None
        and math.isfinite(value)
        and 0.0 <= value <= PAPER_NEW_PAIRS_MOMENTUM_MAX_TOP10_HOLDER_PCT
    )


def _is_ui_age_observed_fresh(value: float | None, maximum: float | None) -> bool:
    return (
        value is not None
        and maximum is not None
        and math.isfinite(value)
        and math.isfinite(maximum)
        and 0.0 <= value <= maximum
        and maximum > 0.0
    )


def _required_evidence_blockers(assessment: RiskAssessment) -> list[str]:
    required_checks = (
        (assessment.mint_authority_check, "paper_momentum_blocked_authority"),
        (assessment.freeze_authority_check, "paper_momentum_blocked_freeze"),
        (assessment.liquidity_check, "paper_momentum_blocked_liquidity"),
    )
    return [label for result, label in required_checks if result != CheckResult.PASS]


def _known_risk_blockers(assessment: RiskAssessment) -> list[str]:
    blockers: list[str] = []
    if assessment.honeypot_check == CheckResult.FAIL:
        blockers.append("paper_momentum_blocked_honeypot")
    if assessment.creator_holding_check == CheckResult.FAIL:
        blockers.append("paper_momentum_blocked_creator")
    if assessment.unique_buyers_check == CheckResult.FAIL:
        blockers.append("paper_momentum_blocked_unique_buyers")
    return blockers
