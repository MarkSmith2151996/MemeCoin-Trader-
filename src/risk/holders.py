"""Holder concentration checks."""

from __future__ import annotations

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo


def check_top10_holders(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.top10_holder_pct is None:
        return CheckResult.UNKNOWN
    if token.top10_holder_pct > config.max_top10_holder_pct:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_creator_holding(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.creator_holding_pct is None:
        return CheckResult.UNKNOWN
    if token.creator_holding_pct > config.max_creator_holding_pct:
        return CheckResult.FAIL
    return CheckResult.PASS
