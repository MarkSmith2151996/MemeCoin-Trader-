from datetime import UTC, datetime, timedelta

from src.core.models import CheckResult, RiskAssessment, TokenInfo
from src.risk.scorer import assess_token


def test_risk_assessment_all_checks_pass() -> None:
    assessment = RiskAssessment(
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.PASS,
    )

    assert assessment.all_checks_pass is True


def test_assess_token_scores_complete_safe_token() -> None:
    token = TokenInfo(
        mint_address="So11111111111111111111111111111111111111112",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        liquidity_sol=20.0,
        unique_buyers=25,
        top10_holder_pct=30.0,
        creator_holding_pct=5.0,
        mint_authority_revoked=True,
        freeze_authority_revoked=True,
    )

    assessment = assess_token(token)

    assert assessment.score == 90.0
    assert assessment.honeypot_check == CheckResult.UNKNOWN
