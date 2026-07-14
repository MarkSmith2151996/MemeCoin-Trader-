"""Focused coverage for the isolated New Pairs paper momentum lane."""

from datetime import UTC, datetime

import pytest

from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.risk.paper_minimum import evaluate_paper_minimum_evidence
from src.risk.paper_momentum import (
    PAPER_NEW_PAIRS_MOMENTUM_MAX_TOP10_HOLDER_PCT,
    evaluate_paper_new_pairs_momentum_evidence,
)
from src.risk.scorer import assess_token


def _signal() -> Signal:
    return Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="PaperMomentumMint1111111111111111111111111111",
        observed_at=datetime.now(UTC),
    )


def _assessment(**updates: CheckResult) -> RiskAssessment:
    checks = {
        "liquidity_check": CheckResult.PASS,
        "top10_holder_check": CheckResult.FAIL,
        "creator_holding_check": CheckResult.UNKNOWN,
        "age_check": CheckResult.PASS,
        "unique_buyers_check": CheckResult.UNKNOWN,
        "mint_authority_check": CheckResult.PASS,
        "freeze_authority_check": CheckResult.PASS,
        "honeypot_check": CheckResult.UNKNOWN,
    }
    checks.update(updates)
    return RiskAssessment(**checks)


def test_momentum_accepts_numeric_concentration_at_or_below_90_percent() -> None:
    result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(), _assessment(), top10_holder_pct=PAPER_NEW_PAIRS_MOMENTUM_MAX_TOP10_HOLDER_PCT
    )

    assert result.eligible is True
    assert "paper_momentum_pass" in result.reason_labels


@pytest.mark.parametrize("concentration", (None, float("nan"), -1.0, 90.000001))
def test_momentum_rejects_missing_invalid_or_over_threshold_concentration(concentration: float | None) -> None:
    result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(), _assessment(), top10_holder_pct=concentration
    )

    assert result.eligible is False
    assert "paper_momentum_blocked_top_holders" in result.reason_labels


def test_momentum_retains_other_required_evidence() -> None:
    result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(), _assessment(liquidity_check=CheckResult.UNKNOWN), top10_holder_pct=80.0
    )

    assert result.reason_labels == ("paper_momentum_blocked_liquidity",)


def test_momentum_does_not_change_the_strict_50_percent_gate() -> None:
    strict_assessment = assess_token(
        TokenInfo(
            mint_address="StrictMomentumMint111111111111111111111111111",
            top10_holder_pct=80.0,
        ),
        RiskConfig(),
    )
    before = strict_assessment.model_dump()

    strict_result = evaluate_paper_minimum_evidence(_signal(), strict_assessment)
    momentum_result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(), strict_assessment, top10_holder_pct=80.0
    )

    assert strict_result.eligible is False
    assert "paper_minimum_blocked_top_holders" in strict_result.reason_labels
    assert RiskConfig().max_top10_holder_pct == 50.0
    assert strict_assessment.model_dump() == before
    assert "paper_momentum_blocked_authority" in momentum_result.reason_labels


@pytest.mark.parametrize(
    ("check_name", "expected_label"),
    (
        ("mint_authority_check", "paper_momentum_blocked_authority"),
        ("freeze_authority_check", "paper_momentum_blocked_freeze"),
        ("liquidity_check", "paper_momentum_blocked_liquidity"),
        ("age_check", "paper_momentum_blocked_age_unknown"),
    ),
)
def test_momentum_retains_non_holder_paper_minimum_checks(
    check_name: str,
    expected_label: str,
) -> None:
    result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(),
        _assessment(**{check_name: CheckResult.UNKNOWN}),
        top10_holder_pct=80.0,
    )

    assert result.eligible is False
    assert expected_label in result.reason_labels


def test_momentum_is_explicit_and_does_not_approve_the_default_assessment() -> None:
    assessment = _assessment(top10_holder_check=CheckResult.FAIL)

    momentum_result = evaluate_paper_new_pairs_momentum_evidence(
        _signal(), assessment, top10_holder_pct=80.0
    )

    assert assessment.all_checks_pass is False
    assert evaluate_paper_minimum_evidence(_signal(), assessment).eligible is False
    assert momentum_result.eligible is True
