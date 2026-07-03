"""Contract authority checks for Solana token mints."""

from __future__ import annotations

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo


def check_mint_authority(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.mint_authority_revoked is None:
        return CheckResult.UNKNOWN
    if config.require_mint_authority_revoked and not token.mint_authority_revoked:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_freeze_authority(token: TokenInfo, config: RiskConfig) -> CheckResult:
    if token.freeze_authority_revoked is None:
        return CheckResult.UNKNOWN
    if config.require_freeze_authority_revoked and not token.freeze_authority_revoked:
        return CheckResult.FAIL
    return CheckResult.PASS


def check_honeypot(_token: TokenInfo) -> CheckResult:
    return CheckResult.UNKNOWN
