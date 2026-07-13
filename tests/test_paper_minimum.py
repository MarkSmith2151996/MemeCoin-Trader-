from datetime import UTC, datetime

import pytest

from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.risk.paper_minimum import PAPER_LAUNCH_RESEARCH_MODE, evaluate_paper_minimum_evidence
from src.risk.scorer import assess_token


def _launch_signal() -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="PaperMinimumLaunchMint111111111111111111111",
        observed_at=datetime.now(UTC),
    )


def _non_launch_signal() -> Signal:
    return Signal(
        source=SignalSource.ONCHAIN,
        type=SignalType.NEW_POOL,
        mint_address="PaperMinimumOnchainMint11111111111111111111",
    )


def _assessment(**updates: CheckResult) -> RiskAssessment:
    checks = {
        "liquidity_check": CheckResult.PASS,
        "top10_holder_check": CheckResult.PASS,
        "creator_holding_check": CheckResult.UNKNOWN,
        "age_check": CheckResult.PASS,
        "unique_buyers_check": CheckResult.UNKNOWN,
        "mint_authority_check": CheckResult.PASS,
        "freeze_authority_check": CheckResult.PASS,
        "honeypot_check": CheckResult.UNKNOWN,
    }
    checks.update(updates)
    return RiskAssessment(**checks)


@pytest.mark.parametrize(
    ("check_name", "label"),
    [
        ("mint_authority_check", "paper_minimum_blocked_authority"),
        ("freeze_authority_check", "paper_minimum_blocked_freeze"),
        ("liquidity_check", "paper_minimum_blocked_liquidity"),
        ("top10_holder_check", "paper_minimum_blocked_top_holders"),
    ],
)
def test_paper_minimum_rejects_required_unknown_evidence(check_name: str, label: str) -> None:
    result = evaluate_paper_minimum_evidence(
        _launch_signal(),
        _assessment(**{check_name: CheckResult.UNKNOWN}),
        research_mode=PAPER_LAUNCH_RESEARCH_MODE,
    )

    assert result.eligible is False
    assert label in result.reason_labels


def test_paper_minimum_rejects_top_holder_failure() -> None:
    result = evaluate_paper_minimum_evidence(
        _launch_signal(),
        _assessment(top10_holder_check=CheckResult.FAIL),
        research_mode=PAPER_LAUNCH_RESEARCH_MODE,
    )

    assert result.reason_labels == ("paper_minimum_blocked_top_holders",)


def test_paper_minimum_defers_allowed_unknowns_for_explicit_launch_research() -> None:
    assessment = _assessment(age_check=CheckResult.FAIL)

    result = evaluate_paper_minimum_evidence(
        _launch_signal(),
        assessment,
        research_mode=PAPER_LAUNCH_RESEARCH_MODE,
    )

    assert result.eligible is True
    assert result.reason_labels == (
        "paper_minimum_pass",
        "paper_minimum_deferred_honeypot_unknown",
        "paper_minimum_deferred_creator_unknown",
        "paper_minimum_deferred_unique_buyers_unknown",
        "paper_minimum_deferred_age_launch_research",
    )
    assert assessment.age_check == CheckResult.FAIL


def test_paper_minimum_does_not_defer_age_without_explicit_launch_research() -> None:
    result = evaluate_paper_minimum_evidence(
        _launch_signal(),
        _assessment(age_check=CheckResult.FAIL),
    )

    assert result.reason_labels == ("paper_minimum_blocked_age",)


def test_paper_minimum_does_not_defer_age_for_non_launch_signals() -> None:
    result = evaluate_paper_minimum_evidence(
        _non_launch_signal(),
        _assessment(age_check=CheckResult.FAIL),
        research_mode=PAPER_LAUNCH_RESEARCH_MODE,
    )

    assert result.reason_labels == ("paper_minimum_blocked_age",)


def test_paper_minimum_blocks_known_honeypot_failure() -> None:
    result = evaluate_paper_minimum_evidence(
        _launch_signal(),
        _assessment(honeypot_check=CheckResult.FAIL),
        research_mode=PAPER_LAUNCH_RESEARCH_MODE,
    )

    assert result.reason_labels == ("paper_minimum_blocked_honeypot",)


def test_paper_minimum_does_not_change_strict_assessment_behavior() -> None:
    strict_assessment = assess_token(
        TokenInfo(mint_address="StrictDefaultMint111111111111111111111111111"),
        RiskConfig(),
    )

    before = strict_assessment.model_dump()
    result = evaluate_paper_minimum_evidence(_launch_signal(), strict_assessment)

    assert strict_assessment.all_checks_pass is False
    assert strict_assessment.mint_authority_check == CheckResult.UNKNOWN
    assert strict_assessment.model_dump() == before
    assert result.eligible is False
    assert "paper_minimum_blocked_authority" in result.reason_labels
