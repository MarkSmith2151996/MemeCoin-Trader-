"""Aggregate token risk scoring."""

from __future__ import annotations

import asyncio
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
from src.risk.funding_analysis import FundingAnalysisResult, FundingTransferProvider, analyze_buyer_funding
from src.risk.funding_provider import HeliusFundingProvider
from src.risk.holders import check_creator_holding, check_top10_holders, filtered_holder_accounts
from src.risk.honeypot_simulation import (
    HoneypotSimulationAdapter,
    HoneypotSimulationRequest,
    HoneypotSimulationResult,
)
from src.risk.liquidity import LiquidityProbe, check_age, check_liquidity, check_unique_buyers
from src.risk.rugcheck import RugCheckClient, RugCheckResult


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
        filtered_accounts = filtered_holder_accounts(largest_accounts)
        for account in filtered_accounts[:10]:
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
    def __init__(
        self,
        config: RiskConfig,
        holder_lookup: ReadOnlyHolderLookup | None = None,
        rugcheck_client: RugCheckClient | None = None,
        funding_provider: FundingTransferProvider | None = None,
        honeypot_adapter: HoneypotSimulationAdapter | None = None,
        liquidity_probe: LiquidityProbe | None = None,
        enable_holder_lookup: bool = True,
        enable_funding_analysis: bool = True,
    ) -> None:
        self._config = config
        self._holder_lookup = holder_lookup or ReadOnlyHolderLookup()
        self._rugcheck_client = rugcheck_client
        self._funding_provider = funding_provider or HeliusFundingProvider()
        self._honeypot_adapter = honeypot_adapter
        self._liquidity_probe = liquidity_probe or LiquidityProbe()
        self._enable_holder_lookup = enable_holder_lookup
        self._enable_funding_analysis = enable_funding_analysis
        self._cache: dict[str, HolderLookupResult | None] = {}
        self._rugcheck_cache: dict[str, RugCheckResult | None] = {}
        self._funding_cache: dict[str, tuple[FundingAnalysisResult, bool] | None] = {}
        self._liquidity_cache: dict[str, dict[str, object]] = {}
        self._lookup_outcomes: Counter[str] = Counter()

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        token = build_token_from_signal(signal)
        token = await self._enrich_liquidity(token)
        rugcheck_result, funding_response = await asyncio.gather(
            self._fetch_rugcheck(token),
            self._analyze_funding(signal, token),
        )

        funding_result, missing_provider = funding_response
        if rugcheck_result is not None or funding_result is not None:
            self._lookup_outcomes["parallel_prechecks_used"] += 1
        token, lookup_status, holder_diagnostics = await self._enrich_token(token, rugcheck_result)
        signal.payload["holder_diagnostics"] = holder_diagnostics
        token = _apply_funding_token_fields(token, funding_result)
        assessment = assess_token(token, self._config)
        assessment = _apply_rugcheck_assessment(assessment, rugcheck_result)
        honeypot_result = await self._simulate_honeypot(signal, rugcheck_result)
        assessment = _apply_honeypot_simulation_assessment(assessment, honeypot_result)
        assessment = _apply_funding_assessment(assessment, funding_result, missing_provider=missing_provider)
        self._record_lookup_outcome(lookup_status, assessment)
        self._record_rugcheck_outcome(rugcheck_result, assessment)
        self._record_honeypot_simulation_outcome(honeypot_result, assessment)
        self._record_funding_outcome(signal, funding_result, missing_provider=missing_provider)
        return assessment

    def diagnostics(self) -> dict[str, int]:
        return dict(sorted(self._lookup_outcomes.items()))

    async def _enrich_token(
        self,
        token: TokenInfo,
        rugcheck_result: RugCheckResult | None,
    ) -> tuple[TokenInfo, str | None, dict[str, object]]:
        rugcheck_top10_holder_pct = _rugcheck_top10_holder_pct(rugcheck_result)
        token = _apply_rugcheck_token_fields(token, rugcheck_result)
        holder_diagnostics = {
            "rugcheck_top10_holder_pct": rugcheck_top10_holder_pct,
            "local_filtered_top10_holder_pct": None,
            "selected_top10_holder_pct": token.top10_holder_pct,
            "top10_holder_source": "signal_payload" if token.top10_holder_pct is not None else "unknown",
            "local_holder_lookup_attempted": False,
            "local_holder_lookup_status": None,
        }

        if token.top10_holder_pct is not None:
            return token, None, holder_diagnostics

        if rugcheck_top10_holder_pct is not None:
            holder_diagnostics["selected_top10_holder_pct"] = rugcheck_top10_holder_pct
            if rugcheck_top10_holder_pct > self._config.max_top10_holder_pct:
                holder_diagnostics["local_holder_lookup_attempted"] = True
                lookup_result = await self._fetch_holder_lookup(token.mint_address)
                lookup_status = "holder_lookup_override_failed_provider" if lookup_result is None else lookup_result.status
                holder_diagnostics["local_holder_lookup_status"] = lookup_status
                if lookup_result is not None and lookup_result.top10_holder_pct is not None:
                    holder_diagnostics["local_filtered_top10_holder_pct"] = lookup_result.top10_holder_pct
                    holder_diagnostics["selected_top10_holder_pct"] = lookup_result.top10_holder_pct
                    holder_diagnostics["top10_holder_source"] = "local_filtered_override"
                    return _apply_holder_lookup_fields(token, lookup_result), "holder_lookup_local_override_succeeded", holder_diagnostics
                holder_diagnostics["top10_holder_source"] = "rugcheck_no_local_override"
                return token.model_copy(update={"top10_holder_pct": rugcheck_top10_holder_pct}), lookup_status, holder_diagnostics

            holder_diagnostics["top10_holder_source"] = "rugcheck"
            return token.model_copy(update={"top10_holder_pct": rugcheck_top10_holder_pct}), None, holder_diagnostics

        if not self._enable_holder_lookup:
            return token, None, holder_diagnostics

        holder_diagnostics["local_holder_lookup_attempted"] = True
        lookup_result = await self._fetch_holder_lookup(token.mint_address)
        lookup_status = "holder_lookup_failed_provider" if lookup_result is None else lookup_result.status
        holder_diagnostics["local_holder_lookup_status"] = lookup_status
        if lookup_result is None:
            return token, lookup_status, holder_diagnostics

        if lookup_result.status != "holder_lookup_succeeded":
            return token, lookup_result.status, holder_diagnostics

        holder_diagnostics["local_filtered_top10_holder_pct"] = lookup_result.top10_holder_pct
        holder_diagnostics["selected_top10_holder_pct"] = lookup_result.top10_holder_pct
        holder_diagnostics["top10_holder_source"] = "local_filtered_lookup"
        return _apply_holder_lookup_fields(token, lookup_result), "holder_lookup_succeeded", holder_diagnostics

    async def _fetch_holder_lookup(self, mint_address: str) -> HolderLookupResult | None:
        if mint_address not in self._cache:
            try:
                self._cache[mint_address] = await self._holder_lookup.fetch(mint_address)
            except Exception:
                self._cache[mint_address] = HolderLookupResult(status="holder_lookup_failed_provider")
        return self._cache[mint_address]

    async def _enrich_liquidity(self, token: TokenInfo) -> TokenInfo:
        if token.liquidity_sol is not None:
            return token

        if token.mint_address not in self._liquidity_cache:
            try:
                self._liquidity_cache[token.mint_address] = await self._liquidity_probe.get_pool_info(token.mint_address)
            except Exception:
                self._liquidity_cache[token.mint_address] = {"pool_liquidity_sol": None, "source": "provider_error"}

        probe_result = self._liquidity_cache[token.mint_address]
        liquidity_sol = probe_result.get("pool_liquidity_sol") if isinstance(probe_result, Mapping) else None
        if not isinstance(liquidity_sol, (int, float)):
            return token
        return token.model_copy(update={"liquidity_sol": float(liquidity_sol)})

    async def _fetch_rugcheck(self, token: TokenInfo) -> RugCheckResult | None:
        if self._rugcheck_client is None:
            return None
        if not _looks_like_solana_mint(token.mint_address):
            return None

        if token.mint_address not in self._rugcheck_cache:
            try:
                self._rugcheck_cache[token.mint_address] = await self._rugcheck_client.fetch_report(token.mint_address)
            except Exception:
                self._rugcheck_cache[token.mint_address] = RugCheckResult(
                    mint_address=token.mint_address,
                    provider_status="provider_error",
                    error="unexpected exception",
                )

        return self._rugcheck_cache[token.mint_address]

    async def _analyze_funding(self, signal: Signal, token: TokenInfo) -> tuple[FundingAnalysisResult | None, bool]:
        if not self._enable_funding_analysis:
            return None, False

        buyer_wallets = _extract_buyer_wallets(signal)
        if buyer_wallets is None:
            return None, False
        if not buyer_wallets:
            return FundingAnalysisResult(
                funding_sybil_check=CheckResult.UNKNOWN,
                bundled_buyer_pct=0.0,
                largest_common_funder_group_size=0,
                buyers_with_known_funders=0,
                buyers_with_unknown_funders=0,
                total_buyers=0,
                flagged=False,
            ), False

        cache_key = f"{token.mint_address}:{'|'.join(sorted(buyer_wallets))}"
        if cache_key not in self._funding_cache:
            provider = _FundingProviderProbe(self._funding_provider)
            try:
                self._funding_cache[cache_key] = (
                    await analyze_buyer_funding(buyer_wallets, provider),
                    provider.missing_provider,
                )
            except Exception:
                self._funding_cache[cache_key] = None

        cached = self._funding_cache[cache_key]
        if cached is None:
            return None, False
        return cached

    async def _simulate_honeypot(
        self,
        signal: Signal,
        rugcheck_result: RugCheckResult | None,
    ) -> HoneypotSimulationResult | None:
        if self._honeypot_adapter is None:
            return None
        if rugcheck_result is not None and rugcheck_result.provider_status == "ok" and rugcheck_result.found and rugcheck_result.is_honeypot is not None:
            return None

        request = _build_honeypot_request(signal)
        if request is None:
            return None

        try:
            return await self._honeypot_adapter.simulate_sell(request)
        except Exception:
            return HoneypotSimulationResult(
                ok=False,
                sell_simulation_passed=False,
                blocked_reason="provider error",
                provider_error="unexpected exception",
                provider_status="provider_error",
                backend=request.backend,
            )

    def _record_lookup_outcome(self, lookup_status: str | None, assessment: RiskAssessment) -> None:
        if lookup_status is None:
            return
        if lookup_status in {"holder_lookup_succeeded", "holder_lookup_local_override_succeeded"} and assessment.top10_holder_check == CheckResult.FAIL:
            self._lookup_outcomes["holder_lookup_threshold_failed"] += 1
            return
        self._lookup_outcomes[lookup_status] += 1

    def _record_rugcheck_outcome(self, rugcheck_result: RugCheckResult | None, assessment: RiskAssessment) -> None:
        if rugcheck_result is None:
            return

        provider_status = rugcheck_result.provider_status or "unknown"
        if provider_status != "ok" or not rugcheck_result.found:
            self._lookup_outcomes[f"rugcheck_failed_{provider_status}"] += 1
            return

        self._lookup_outcomes["rugcheck_used"] += 1
        if rugcheck_result.top_holder_pct is not None:
            self._lookup_outcomes["rugcheck_used_top_holder_pct"] += 1
        if rugcheck_result.mint_authority_revoked is not None:
            self._lookup_outcomes["rugcheck_used_mint_authority"] += 1
        if rugcheck_result.freeze_authority_revoked is not None:
            self._lookup_outcomes["rugcheck_used_freeze_authority"] += 1
        if rugcheck_result.is_honeypot is not None:
            outcome = "fail" if assessment.honeypot_check == CheckResult.FAIL else "pass"
            self._lookup_outcomes[f"rugcheck_used_honeypot_{outcome}"] += 1
        if rugcheck_result.risk_level:
            self._lookup_outcomes[f"rugcheck_risk_level_{rugcheck_result.risk_level.lower()}"] += 1

    def _record_honeypot_simulation_outcome(
        self,
        honeypot_result: HoneypotSimulationResult | None,
        assessment: RiskAssessment,
    ) -> None:
        if honeypot_result is None:
            return
        self._lookup_outcomes["honeypot_simulation_used"] += 1
        if not honeypot_result.ok:
            self._lookup_outcomes[f"honeypot_simulation_{honeypot_result.provider_status}"] += 1
            return
        if assessment.honeypot_check == CheckResult.FAIL:
            self._lookup_outcomes["honeypot_simulation_blocked"] += 1
            return
        if assessment.honeypot_check == CheckResult.PASS:
            self._lookup_outcomes["honeypot_simulation_passed"] += 1

    def _record_funding_outcome(
        self,
        signal: Signal,
        funding_result: FundingAnalysisResult | None,
        *,
        missing_provider: bool,
    ) -> None:
        buyer_wallets = _extract_buyer_wallets(signal)
        if buyer_wallets is None:
            return
        if not buyer_wallets:
            self._lookup_outcomes["funding_analysis_missing_buyers"] += 1
            return
        if funding_result is None:
            self._lookup_outcomes["funding_analysis_unknown"] += 1
            return

        self._lookup_outcomes["funding_analysis_used"] += 1
        if missing_provider:
            self._lookup_outcomes["funding_analysis_missing_provider"] += 1
        if funding_result.funding_sybil_check == CheckResult.FAIL:
            self._lookup_outcomes["funding_analysis_failed_threshold"] += 1
            return
        if funding_result.funding_sybil_check == CheckResult.PASS:
            self._lookup_outcomes["funding_analysis_passed"] += 1
            return
        self._lookup_outcomes["funding_analysis_unknown"] += 1


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


def _apply_rugcheck_token_fields(token: TokenInfo, rugcheck_result: RugCheckResult | None) -> TokenInfo:
    if rugcheck_result is None or rugcheck_result.provider_status != "ok" or not rugcheck_result.found:
        return token

    updates: dict[str, object] = {}
    if token.mint_authority_revoked is None and rugcheck_result.mint_authority_revoked is not None:
        updates["mint_authority_revoked"] = rugcheck_result.mint_authority_revoked
    if token.freeze_authority_revoked is None and rugcheck_result.freeze_authority_revoked is not None:
        updates["freeze_authority_revoked"] = rugcheck_result.freeze_authority_revoked

    if not updates:
        return token
    return token.model_copy(update=updates)


def _rugcheck_top10_holder_pct(rugcheck_result: RugCheckResult | None) -> float | None:
    if rugcheck_result is None or rugcheck_result.provider_status != "ok" or not rugcheck_result.found:
        return None
    return rugcheck_result.top_holder_pct


def _apply_holder_lookup_fields(token: TokenInfo, lookup_result: HolderLookupResult) -> TokenInfo:
    updates: dict[str, float | int] = {}
    if lookup_result.top10_holder_pct is not None:
        updates["top10_holder_pct"] = lookup_result.top10_holder_pct
    if token.creator_holding_pct is None and lookup_result.creator_holding_pct is not None:
        updates["creator_holding_pct"] = lookup_result.creator_holding_pct
    if token.holder_count is None and lookup_result.holder_count is not None:
        updates["holder_count"] = lookup_result.holder_count
    if not updates:
        return token
    return token.model_copy(update=updates)


def _apply_rugcheck_assessment(
    assessment: RiskAssessment,
    rugcheck_result: RugCheckResult | None,
) -> RiskAssessment:
    if rugcheck_result is None or rugcheck_result.provider_status != "ok" or not rugcheck_result.found:
        return assessment
    if rugcheck_result.is_honeypot is None:
        return assessment

    honeypot_check = CheckResult.FAIL if rugcheck_result.is_honeypot else CheckResult.PASS
    reasons = [reason for reason in assessment.reasons if not reason.startswith("honeypot_check ")]
    score = assessment.score
    if honeypot_check == CheckResult.PASS and assessment.honeypot_check != CheckResult.PASS:
        score += CHECK_WEIGHTS["honeypot_check"]
    elif honeypot_check == CheckResult.FAIL and assessment.honeypot_check == CheckResult.PASS:
        score -= CHECK_WEIGHTS["honeypot_check"]
    if honeypot_check == CheckResult.FAIL:
        reasons.append("honeypot_check failed")
    return assessment.model_copy(update={"honeypot_check": honeypot_check, "score": score, "reasons": reasons})


def _apply_honeypot_simulation_assessment(
    assessment: RiskAssessment,
    honeypot_result: HoneypotSimulationResult | None,
) -> RiskAssessment:
    if honeypot_result is None or not honeypot_result.ok:
        return assessment

    honeypot_check = CheckResult.PASS if honeypot_result.sell_simulation_passed else CheckResult.FAIL
    current = assessment.honeypot_check
    if current == CheckResult.FAIL:
        return assessment

    reasons = [reason for reason in assessment.reasons if not reason.startswith("honeypot_check ")]
    score = assessment.score
    if honeypot_check == CheckResult.PASS and current != CheckResult.PASS:
        score += CHECK_WEIGHTS["honeypot_check"]
    elif honeypot_check == CheckResult.FAIL and current == CheckResult.PASS:
        score -= CHECK_WEIGHTS["honeypot_check"]

    if honeypot_check == CheckResult.FAIL:
        reasons.append("honeypot_check failed")
    return assessment.model_copy(update={"honeypot_check": honeypot_check, "score": score, "reasons": reasons})


def _apply_funding_token_fields(token: TokenInfo, funding_result: FundingAnalysisResult | None) -> TokenInfo:
    if funding_result is None or funding_result.total_buyers <= 0 or token.unique_buyers is not None:
        return token
    return token.model_copy(update={"unique_buyers": funding_result.total_buyers})


def _apply_funding_assessment(
    assessment: RiskAssessment,
    funding_result: FundingAnalysisResult | None,
    *,
    missing_provider: bool,
) -> RiskAssessment:
    if funding_result is None:
        return assessment
    if funding_result.total_buyers == 0 and not missing_provider:
        return assessment

    funding_check = funding_result.funding_sybil_check
    if funding_check == CheckResult.PASS and assessment.unique_buyers_check != CheckResult.FAIL:
        return assessment

    reasons = [reason for reason in assessment.reasons if not reason.startswith("unique_buyers_check ")]
    score = assessment.score
    current = assessment.unique_buyers_check

    if funding_check == CheckResult.FAIL:
        if current == CheckResult.PASS:
            score -= CHECK_WEIGHTS["unique_buyers_check"]
        reasons.append("unique_buyers_check failed")
        return assessment.model_copy(update={"unique_buyers_check": CheckResult.FAIL, "score": score, "reasons": reasons})

    if current == CheckResult.FAIL:
        return assessment

    if current == CheckResult.PASS:
        score -= CHECK_WEIGHTS["unique_buyers_check"]
    reasons.append("unique_buyers_check unknown")
    if missing_provider or funding_check == CheckResult.UNKNOWN:
        return assessment.model_copy(update={"unique_buyers_check": CheckResult.UNKNOWN, "score": score, "reasons": reasons})
    return assessment


def _looks_like_solana_mint(mint_address: str) -> bool:
    normalized = mint_address.strip()
    if not 32 <= len(normalized) <= 44:
        return False
    return all(ch in "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz" for ch in normalized)


class _FundingProviderProbe:
    def __init__(self, provider: FundingTransferProvider) -> None:
        self._provider = provider
        self.missing_provider = False

    async def get_recent_inbound_transfers(self, wallet: str):
        lookup_wallet = getattr(self._provider, "lookup_wallet", None)
        if callable(lookup_wallet):
            result = await lookup_wallet(wallet)
            provider_status = getattr(result, "provider_status", "unknown")
            if provider_status == "missing_api_key":
                self.missing_provider = True
                return None
            if provider_status != "ok":
                return None
            return getattr(result, "transfers", None)
        return await self._provider.get_recent_inbound_transfers(wallet)


def _extract_buyer_wallets(signal: Signal) -> list[str] | None:
    payload = signal.payload
    candidates: list[object] = [payload]
    for field_name in ("token", "coin"):
        nested = payload.get(field_name)
        if isinstance(nested, Mapping):
            candidates.insert(0, nested)

    extracted: list[str] = []
    saw_field = False
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        field_names = (
            "buyer_wallets",
            "buyerWallets",
            "buyers",
            "buyerAddresses",
            "buyer_addresses",
            "uniqueBuyerWallets",
            "recentBuyers",
        )
        raw_wallets = _first_value([candidate], *field_names)
        if any(candidate.get(field_name) is not None for field_name in field_names):
            saw_field = True
        extracted.extend(_normalize_wallet_list(raw_wallets))

    deduped: list[str] = []
    seen: set[str] = set()
    for wallet in extracted:
        if wallet in seen:
            continue
        seen.add(wallet)
        deduped.append(wallet)
    if deduped:
        return deduped
    return [] if saw_field else None


def _normalize_wallet_list(value: object) -> list[str]:
    if isinstance(value, str):
        wallet = value.strip()
        return [wallet] if wallet else []
    if isinstance(value, Mapping):
        for key in ("wallet", "address", "buyer", "owner", "traderPublicKey", "user"):
            nested = value.get(key)
            if isinstance(nested, str) and nested.strip():
                return [nested.strip()]
        normalized: list[str] = []
        for nested in value.values():
            normalized.extend(_normalize_wallet_list(nested))
        return normalized
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            normalized.extend(_normalize_wallet_list(item))
        return normalized
    return []


def _build_honeypot_request(signal: Signal) -> HoneypotSimulationRequest | None:
    payload = signal.payload
    candidates: list[object] = [payload]
    for field_name in ("token", "coin"):
        nested = payload.get(field_name)
        if isinstance(nested, Mapping):
            candidates.insert(0, nested)

    transaction_payload = _first_value(
        [candidate for candidate in candidates if isinstance(candidate, Mapping)],
        "sellTransactionPayload",
        "sell_transaction_payload",
        "serializedSellTx",
        "serialized_sell_tx",
        "transactionPayload",
        "transaction_payload",
    )
    if not isinstance(transaction_payload, (str, bytes)) or (isinstance(transaction_payload, str) and not transaction_payload.strip()) or (isinstance(transaction_payload, bytes) and len(transaction_payload) == 0):
        return None

    parsed_instructions_raw = _first_value(
        [candidate for candidate in candidates if isinstance(candidate, Mapping)],
        "parsedInstructions",
        "parsed_instructions",
        "sellParsedInstructions",
        "sell_parsed_instructions",
    )
    parsed_instructions: tuple[Mapping[str, object], ...] = ()
    if isinstance(parsed_instructions_raw, list):
        parsed_instructions = tuple(item for item in parsed_instructions_raw if isinstance(item, Mapping))

    backend = _first_str(
        [candidate for candidate in candidates if isinstance(candidate, Mapping)],
        "simulationBackend",
        "simulation_backend",
        "honeypotBackend",
        "honeypot_backend",
    ) or "helius"
    return HoneypotSimulationRequest(
        mint_address=signal.mint_address,
        transaction_payload=transaction_payload,
        backend=backend,
        parsed_instructions=parsed_instructions,
    )


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
