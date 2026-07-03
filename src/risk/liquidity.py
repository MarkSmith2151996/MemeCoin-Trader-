"""Liquidity, age, and buyer checks."""

from __future__ import annotations

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo


def check_liquidity(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.liquidity_sol is None:
        return CheckResult.UNKNOWN
    if token.liquidity_sol < config.min_liquidity_sol:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_age(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.age_minutes is None:
        return CheckResult.UNKNOWN
    if token.age_minutes < config.min_age_minutes:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_unique_buyers(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.unique_buyers is None:
        return CheckResult.UNKNOWN
    if token.unique_buyers < config.min_unique_buyers:
        return CheckResult.FAIL
    return CheckResult.PASS
