"""Aggregate token risk scoring."""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from dotenv import dotenv_values

from src.chain.rpc import SolanaRpcClient
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


@dataclass(slots=True)
class HolderLookupResult:
    status: str = "holder_lookup_succeeded"
    top10_holder_pct: float | None = None
    creator_holding_pct: float | None = None
    holder_count: int | None = None


class ReadOnlyHolderLookup:
    def __init__(
        self,
        rpc_url: str | None = None,
        timeout_s: float = 10.0,
        rpc_client_factory: Callable[[str, float], SolanaRpcClient] | None = None,
        dotenv_path: str | Path | None = None,
    ) -> None:
        self._rpc_url = rpc_url or resolve_read_only_rpc_url(dotenv_path=dotenv_path)
        self._timeout_s = timeout_s
        self._rpc_client_factory = rpc_client_factory or SolanaRpcClient

    async def fetch(self, mint_address: str) -> HolderLookupResult | None:
        if not mint_address.strip():
            return HolderLookupResult(status="holder_lookup_skipped_missing_mint")
        if not self._rpc_url:
            return HolderLookupResult(status="holder_lookup_failed_provider")

        client = self._rpc_client_factory(self._rpc_url, self._timeout_s)
        try:
            supply_result = await client.call("getTokenSupply", [mint_address])
            largest_accounts_result = await client.call("getTokenLargestAccounts", [mint_address])
        finally:
            await client.close()

        supply = _extract_token_balance((supply_result or {}).get("value") if isinstance(supply_result, Mapping) else supply_result)
        if supply is None or supply <= 0:
            return HolderLookupResult(status="holder_lookup_no_supply")

        largest_accounts = (largest_accounts_result or {}).get("value") if isinstance(largest_accounts_result, Mapping) else largest_accounts_result
        if not isinstance(largest_accounts, list) or not largest_accounts:
            return HolderLookupResult(status="holder_lookup_no_largest_accounts")

        top10_total = 0.0
        for account in largest_accounts[:10]:
            if not isinstance(account, Mapping):
                continue
            balance = _extract_token_balance(account)
            if balance is not None:
                top10_total += balance

        if top10_total <= 0:
            return HolderLookupResult(status="holder_lookup_no_largest_accounts")

        return HolderLookupResult(
            status="holder_lookup_succeeded",
            top10_holder_pct=round((top10_total / supply) * 100, 6),
        )


class DiscoveryRiskScorer:
    def __init__(self, config: RiskConfig, holder_lookup: ReadOnlyHolderLookup | None = None) -> None:
        self._config = config
        self._holder_lookup = holder_lookup or ReadOnlyHolderLookup()
        self._cache: dict[str, HolderLookupResult | None] = {}
        self._lookup_outcomes: Counter[str] = Counter()

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        token = build_token_from_signal(signal)
        token, lookup_status = await self._enrich_token(token)
        assessment = assess_token(token, self._config)
        self._record_lookup_outcome(lookup_status, assessment)
        return assessment

    def diagnostics(self) -> dict[str, int]:
        return dict(sorted(self._lookup_outcomes.items()))

    async def _enrich_token(self, token: TokenInfo) -> tuple[TokenInfo, str | None]:
        if token.top10_holder_pct is not None:
            return token, None

        if token.mint_address not in self._cache:
            try:
                self._cache[token.mint_address] = await self._holder_lookup.fetch(token.mint_address)
            except Exception:
                self._cache[token.mint_address] = HolderLookupResult(status="holder_lookup_failed_provider")

        lookup_result = self._cache[token.mint_address]
        if lookup_result is None:
            return token, "holder_lookup_failed_provider"

        if lookup_result.status != "holder_lookup_succeeded":
            return token, lookup_result.status

        updates: dict[str, float | int] = {}
        if token.top10_holder_pct is None and lookup_result.top10_holder_pct is not None:
            updates["top10_holder_pct"] = lookup_result.top10_holder_pct
        if token.creator_holding_pct is None and lookup_result.creator_holding_pct is not None:
            updates["creator_holding_pct"] = lookup_result.creator_holding_pct
        if token.holder_count is None and lookup_result.holder_count is not None:
            updates["holder_count"] = lookup_result.holder_count
        if not updates:
            return token, "holder_lookup_succeeded"
        return token.model_copy(update=updates), "holder_lookup_succeeded"

    def _record_lookup_outcome(self, lookup_status: str | None, assessment: RiskAssessment) -> None:
        if lookup_status is None:
            return
        if lookup_status == "holder_lookup_succeeded" and assessment.top10_holder_check == CheckResult.FAIL:
            self._lookup_outcomes["holder_lookup_threshold_failed"] += 1
            return
        self._lookup_outcomes[lookup_status] += 1


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


def resolve_read_only_rpc_url(dotenv_path: str | Path | None = None) -> str:
    direct_rpc_url = os.getenv("HELIUS_RPC_URL", "").strip()
    if direct_rpc_url:
        return direct_rpc_url

    direct_api_key = os.getenv("HELIUS_API_KEY", "").strip()
    if direct_api_key:
        return f"https://mainnet.helius-rpc.com/?api-key={direct_api_key}"

    resolved_dotenv_path = Path(dotenv_path) if dotenv_path is not None else Path(__file__).resolve().parents[2] / ".env"
    if not resolved_dotenv_path.exists():
        return ""

    dotenv_data = dotenv_values(resolved_dotenv_path)
    rpc_url = dotenv_data.get("HELIUS_RPC_URL")
    if isinstance(rpc_url, str) and rpc_url.strip():
        return rpc_url.strip()

    api_key = dotenv_data.get("HELIUS_API_KEY")
    if isinstance(api_key, str) and api_key.strip():
        return f"https://mainnet.helius-rpc.com/?api-key={api_key.strip()}"

    return ""


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


def _extract_token_balance(value: object) -> float | None:
    if not isinstance(value, Mapping):
        return None

    ui_amount = value.get("uiAmount")
    if isinstance(ui_amount, (int, float)):
        return float(ui_amount)

    ui_amount_string = value.get("uiAmountString")
    if isinstance(ui_amount_string, str) and ui_amount_string.strip():
        try:
            return float(ui_amount_string)
        except ValueError:
            return None

    raw_amount = value.get("amount")
    decimals = value.get("decimals")
    try:
        if raw_amount is not None and decimals is not None:
            return float(raw_amount) / (10 ** int(decimals))
    except (TypeError, ValueError, ZeroDivisionError):
        return None

    return None
