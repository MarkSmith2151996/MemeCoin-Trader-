"""Holder concentration checks and holder-account filtering helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo

KNOWN_NON_PERSON_OWNERS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  # Orca Whirlpool
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium authority
    "11111111111111111111111111111111",  # System program
    "1nc1nerator11111111111111111111111111111111",  # Incinerator
}

KNOWN_NON_PERSON_ADDRESSES = {
    "1nc1nerator11111111111111111111111111111111",
}


def _extract_string(account: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = account.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def is_non_person_holder(account: Mapping[str, object]) -> bool:
    """Return True when a largest-account entry is clearly protocol-owned or burned."""

    address = _extract_string(account, "address", "account", "pubkey")
    owner = _extract_string(account, "owner", "ownerAddress", "accountOwner")

    if address in KNOWN_NON_PERSON_ADDRESSES:
        return True
    if owner in KNOWN_NON_PERSON_OWNERS:
        return True
    if address in KNOWN_NON_PERSON_OWNERS:
        return True
    return False


def classify_holder_account(
    account: Mapping[str, object],
    *,
    extra_excluded_addresses: set[str] | None = None,
) -> str:
    address = _extract_string(account, "address", "account", "pubkey")
    owner = _extract_string(account, "owner", "ownerAddress", "accountOwner")
    if address in KNOWN_NON_PERSON_ADDRESSES:
        return "burn_address"
    if owner in KNOWN_NON_PERSON_OWNERS:
        return "known_program_owner"
    if address in KNOWN_NON_PERSON_OWNERS:
        return "known_program_address"
    if address and extra_excluded_addresses and address in extra_excluded_addresses:
        return "bonding_curve_artifact"
    return "retained"


def analyze_holder_accounts(
    accounts: Sequence[object],
    *,
    extra_excluded_addresses: set[str] | None = None,
) -> tuple[list[Mapping[str, object]], dict[str, object]]:
    filtered: list[Mapping[str, object]] = []
    filtered_out: list[dict[str, str]] = []
    for account in accounts:
        if not isinstance(account, Mapping):
            continue
        classification = classify_holder_account(account, extra_excluded_addresses=extra_excluded_addresses)
        if classification != "retained":
            filtered_out.append(
                {
                    "address": _extract_string(account, "address", "account", "pubkey") or "unknown",
                    "classification": classification,
                }
            )
            continue
        filtered.append(account)

    diagnostics = {
        "raw_account_count": len([account for account in accounts if isinstance(account, Mapping)]),
        "filtered_account_count": len(filtered_out),
        "retained_account_count": len(filtered),
        "top_filtered_accounts": filtered_out[:3],
        "top_retained_accounts": [
            {
                "address": _extract_string(account, "address", "account", "pubkey") or "unknown",
                "classification": "retained",
            }
            for account in filtered[:3]
        ],
    }
    return filtered, diagnostics


def filtered_holder_accounts(
    accounts: Sequence[object],
    *,
    extra_excluded_addresses: set[str] | None = None,
) -> list[Mapping[str, object]]:
    filtered, _ = analyze_holder_accounts(accounts, extra_excluded_addresses=extra_excluded_addresses)
    return filtered


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
