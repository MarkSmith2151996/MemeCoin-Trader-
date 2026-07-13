"""Holder concentration checks and holder-account filtering helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo

SYSTEM_PROGRAM_ADDRESS = "11111111111111111111111111111111"
INCINERATOR_ADDRESS = "1nc1nerator11111111111111111111111111111111"
RAYDIUM_AMM_PROGRAM_ADDRESS = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_AMM_AUTHORITY_ADDRESS = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"

KNOWN_NON_PERSON_OWNERS = {
    RAYDIUM_AMM_PROGRAM_ADDRESS,
    RAYDIUM_AMM_AUTHORITY_ADDRESS,
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  # Orca Whirlpool
    SYSTEM_PROGRAM_ADDRESS,
    INCINERATOR_ADDRESS,
}

KNOWN_RAYDIUM_IDENTITIES = {
    RAYDIUM_AMM_PROGRAM_ADDRESS,
    RAYDIUM_AMM_AUTHORITY_ADDRESS,
}


def _extract_string(account: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = account.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def is_non_person_holder(account: Mapping[str, object]) -> bool:
    """Return True when a largest-account entry is clearly protocol-owned or burned."""

    return classify_holder_account(account) != "retained"


def _is_raydium_pool_or_pda(account: Mapping[str, object]) -> bool:
    program_identity = _extract_string(
        account,
        "programId",
        "program_id",
        "ownerProgram",
        "owner_program",
    )
    if program_identity not in KNOWN_RAYDIUM_IDENTITIES:
        return False

    if account.get("isPda") is True or account.get("is_pda") is True:
        return True

    account_type = _extract_string(account, "accountType", "account_type", "type", "kind")
    return account_type is not None and account_type.lower() in {"pool", "lp_pool", "vault", "amm_pool"}


def classify_holder_account(
    account: Mapping[str, object],
    *,
    extra_excluded_addresses: set[str] | None = None,
) -> str:
    address = _extract_string(account, "address", "account", "pubkey")
    owner = _extract_string(account, "owner", "ownerAddress", "accountOwner")
    if address == INCINERATOR_ADDRESS:
        return "burn_address"
    if address == SYSTEM_PROGRAM_ADDRESS:
        return "null_or_system_address"
    if owner == SYSTEM_PROGRAM_ADDRESS:
        return "system_program_owner"
    if _is_raydium_pool_or_pda(account):
        return "raydium_pool_or_pda"
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
