"""Aggregate token risk scoring."""

from __future__ import annotations

from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, TokenInfo
from src.risk.contract_audit import check_freeze_authority, check_honeypot, check_mint_authority
from src.risk.holders import check_creator_holding, check_top10_holders
from src.risk.liquidity import check_age, check_liquidity, check_unique_buyers


CHECK_WEIGHTS = {
    "liquidity_check": 20.0,
    "top10_holder_check": 15.0,
    "creator_holding_check": 15.0,
    "age_check": 10.0,
    "unique_buyers_check": 10.0,
    "mint_authority_check": 10.0,
    "freeze_authority_check": 10.0,
    "honeypot_check": 10.0,
}


def assess_token(token: TokenInfo, config: RiskConfig | None = None) -> RiskAssessment:
    config = config or RiskConfig()
    assessment = RiskAssessment(
        token=token,
        liquidity_check=check_liquidity(token, config),
        top10_holder_check=check_top10_holders(token, config),
        creator_holding_check=check_creator_holding(token, config),
        age_check=check_age(token, config),
        unique_buyers_check=check_unique_buyers(token, config),
        mint_authority_check=check_mint_authority(token, config),
        freeze_authority_check=check_freeze_authority(token, config),
        honeypot_check=check_honeypot(token),
    )
    score = 0.0
    reasons: list[str] = []
    for field_name, weight in CHECK_WEIGHTS.items():
        result = getattr(assessment, field_name)
        if result == CheckResult.PASS:
            score += weight
        elif result == CheckResult.FAIL:
            reasons.append(f"{field_name} failed")
        else:
            reasons.append(f"{field_name} unknown")
    return assessment.model_copy(update={"score": score, "reasons": reasons})
