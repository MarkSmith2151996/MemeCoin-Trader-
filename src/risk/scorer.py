"""Aggregate token risk scoring."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
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


def assess_signal(signal: Signal, config: RiskConfig | None = None) -> RiskAssessment:
    return assess_token(build_token_from_signal(signal), config)


def build_token_from_signal(signal: Signal) -> TokenInfo:
    payload = signal.payload
    candidates = [payload]
    for field_name in ("token", "coin"):
        nested = payload.get(field_name)
        if isinstance(nested, Mapping):
            candidates.insert(0, nested)

    return TokenInfo(
        mint_address=signal.mint_address,
        symbol=_first_str(candidates, "symbol", "ticker"),
        name=_first_str(candidates, "name"),
        creator_address=_first_str(candidates, "creatorAddress", "creator_address", "creator", "traderPublicKey"),
        created_at=_created_at_from_signal(signal, candidates),
        liquidity_sol=_first_float(
            candidates,
            "liquidity_sol",
            "liquiditySol",
            "liquiditySOL",
            "liquidity",
            "solLiquidity",
            "poolLiquiditySol",
            "vSolInBondingCurve",
            "virtualSolReserves",
            "virtual_sol_reserves",
        ),
        market_cap_usd=_first_float(candidates, "market_cap_usd", "marketCapUsd", "usdMarketCap", "marketCapUSD"),
        holder_count=_first_int(candidates, "holder_count", "holderCount", "holders", "total_holders", "totalHolders"),
        unique_buyers=_first_int(candidates, "unique_buyers", "uniqueBuyers", "buyerCount", "uniqueBuyerCount"),
        top10_holder_pct=_first_float(
            candidates,
            "top10_holder_pct",
            "top_10_holder_pct",
            "top10_holders_pct",
            "top10HolderPct",
            "top10HoldersPct",
            "top10HolderPercent",
            "top10_holders_percentage",
            "holderConcentrationTop10Pct",
        ),
        creator_holding_pct=_first_float(
            candidates,
            "creator_holding_pct",
            "creator_holding_percent",
            "creatorHoldingPct",
            "creatorHoldingPercent",
            "creatorPercent",
            "creator_percentage",
            "devHoldingPct",
            "devHoldingPercent",
        ),
        mint_authority_revoked=_first_bool(candidates, "mint_authority_revoked", "mintAuthorityRevoked"),
        freeze_authority_revoked=_first_bool(candidates, "freeze_authority_revoked", "freezeAuthorityRevoked"),
    )


def _created_at_from_signal(signal: Signal, candidates: list[Mapping[str, object]]) -> datetime | None:
    raw_created_at = _first_value(
        candidates,
        "created_at",
        "createdAt",
        "createdTimestamp",
        "timestamp",
        "time",
        "blockTime",
    )
    parsed = _coerce_datetime(raw_created_at)
    if parsed is not None:
        return parsed
    if signal.source == SignalSource.PUMP_FUN and signal.type == SignalType.NEW_POOL:
        return signal.observed_at
    return None


def _first_value(candidates: list[Mapping[str, object]], *field_names: str) -> object | None:
    for field_name in field_names:
        for candidate in candidates:
            value = candidate.get(field_name)
            if value is not None:
                return value
    return None


def _first_str(candidates: list[Mapping[str, object]], *field_names: str) -> str | None:
    value = _first_value(candidates, *field_names)
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _first_float(candidates: list[Mapping[str, object]], *field_names: str) -> float | None:
    value = _first_value(candidates, *field_names)
    if isinstance(value, bool):
        return None
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_int(candidates: list[Mapping[str, object]], *field_names: str) -> int | None:
    value = _first_value(candidates, *field_names)
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_bool(candidates: list[Mapping[str, object]], *field_names: str) -> bool | None:
    value = _first_value(candidates, *field_names)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None
