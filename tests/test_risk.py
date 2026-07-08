import asyncio
from datetime import UTC, datetime, timedelta
from time import monotonic

from src.core.config import RiskConfig
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.risk.funding_analysis import FundingAnalysisResult, InboundTransfer
from src.risk.honeypot_simulation import HoneypotSimulationAdapter, HoneypotSimulationRequest
from src.risk.rugcheck import RugCheckResult
from src.risk.scorer import (
    DiscoveryRiskScorer,
    HolderLookupResult,
    ReadOnlyHolderLookup,
    assess_signal,
    assess_token,
    build_token_from_signal,
)


class FakeRpcClient:
    def __init__(self, responses: dict[str, object]) -> None:
        self._responses = responses
        self.closed = False

    async def call(self, method: str, params: list[object] | None = None) -> object:
        return self._responses[method]

    async def close(self) -> None:
        self.closed = True


class FakeHolderLookup:
    def __init__(self, result: HolderLookupResult | None = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    async def fetch(self, mint_address: str) -> HolderLookupResult | None:
        if self._error is not None:
            raise self._error
        return self._result


class FakeRugCheckClient:
    def __init__(self, result: RugCheckResult | None = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    async def fetch_report(self, mint_address: str) -> RugCheckResult:
        if self._error is not None:
            raise self._error
        if self._result is not None:
            return self._result
        return RugCheckResult(mint_address=mint_address, provider_status="timeout", error="timed out")


class FakeFundingProvider:
    def __init__(self, transfers_by_wallet: dict[str, list[InboundTransfer] | None], failures: set[str] | None = None) -> None:
        self._transfers_by_wallet = transfers_by_wallet
        self._failures = failures or set()

    async def get_recent_inbound_transfers(self, wallet: str) -> list[InboundTransfer] | None:
        if wallet in self._failures:
            raise RuntimeError("provider boom")
        return self._transfers_by_wallet.get(wallet, [])


class MissingProviderFundingProvider:
    async def lookup_wallet(self, wallet: str):
        class Result:
            provider_status = "missing_api_key"
            transfers = None

        return Result()


class SlowRugCheckClient(FakeRugCheckClient):
    def __init__(self, delay_s: float, result: RugCheckResult) -> None:
        super().__init__(result=result)
        self._delay_s = delay_s

    async def fetch_report(self, mint_address: str) -> RugCheckResult:
        await asyncio.sleep(self._delay_s)
        return await super().fetch_report(mint_address)


class SlowFundingProvider(FakeFundingProvider):
    def __init__(self, delay_s: float, transfers_by_wallet: dict[str, list[InboundTransfer] | None]) -> None:
        super().__init__(transfers_by_wallet)
        self._delay_s = delay_s

    async def get_recent_inbound_transfers(self, wallet: str) -> list[InboundTransfer] | None:
        await asyncio.sleep(self._delay_s)
        return await super().get_recent_inbound_transfers(wallet)


class RecordingSimulationProvider:
    def __init__(self, result: object, delay_s: float = 0.0) -> None:
        self.result = result
        self.delay_s = delay_s
        self.calls: list[tuple[str, HoneypotSimulationRequest]] = []

    async def __call__(self, backend: str, request: HoneypotSimulationRequest) -> object:
        self.calls.append((backend, request))
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


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


def test_build_token_from_pump_fun_signal_enriches_liquidity_fields() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="pump-mint",
        payload={
            "symbol": "PUMP",
            "name": "Pump Token",
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "top10HolderPct": 30.0,
            "creatorHoldingPct": 5.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )

    token = build_token_from_signal(signal)

    assert token.mint_address == signal.mint_address
    assert token.symbol == "PUMP"
    assert token.liquidity_sol == 30.1
    assert token.unique_buyers == 25
    assert token.top10_holder_pct == 30.0
    assert token.creator_holding_pct == 5.0
    assert token.mint_authority_revoked is True
    assert token.freeze_authority_revoked is True
    assert token.created_at is not None


def test_build_token_from_signal_maps_holder_alias_variants() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="holder-mint",
        payload={
            "top10HolderPercent": 41.5,
            "creatorPercent": 7.25,
            "totalHolders": 1234,
        },
    )

    token = build_token_from_signal(signal)

    assert token.top10_holder_pct == 41.5
    assert token.creator_holding_pct == 7.25
    assert token.holder_count == 1234


def test_read_only_holder_lookup_computes_top10_holder_pct_from_rpc() -> None:
    rpc_client = FakeRpcClient(
        {
            "getTokenSupply": {"value": {"uiAmount": 100.0}},
            "getTokenLargestAccounts": {
                "value": [
                    {"uiAmount": 30.0},
                    {"uiAmount": 20.0},
                    {"uiAmount": 10.0},
                ]
            },
        }
    )
    lookup = ReadOnlyHolderLookup(
        rpc_url="https://example.invalid",
        rpc_client_factory=lambda _url, _timeout: rpc_client,
    )

    result = asyncio.run(lookup.fetch("mint"))

    assert result is not None
    assert result.status == "holder_lookup_succeeded"
    assert result.top10_holder_pct == 60.0
    assert rpc_client.closed is True


def test_assess_signal_uses_enriched_pump_fun_liquidity() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="pump-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "top10HolderPct": 30.0,
            "creatorHoldingPct": 5.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )

    assessment = assess_signal(signal)

    assert assessment.liquidity_check == CheckResult.PASS
    assert assessment.age_check == CheckResult.PASS
    assert assessment.unique_buyers_check == CheckResult.PASS


def test_assess_signal_keeps_holder_check_unknown_when_holder_fields_missing() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="missing-holder-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )

    assessment = assess_signal(signal)

    assert assessment.top10_holder_check == CheckResult.UNKNOWN
    assert assessment.creator_holding_check == CheckResult.UNKNOWN


def test_discovery_risk_scorer_populates_top10_holder_pct_from_lookup() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="lookup-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "creatorHoldingPct": 5.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=30.0)),
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.PASS
    assert assessment.creator_holding_check == CheckResult.PASS
    assert scorer.diagnostics() == {"holder_lookup_succeeded": 1}


def test_rugcheck_safe_data_populates_existing_scorer_fields() -> None:
    mint_address = "So11111111111111111111111111111111111111112"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(error=RuntimeError("should not be used")),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                liquidity_locked=True,
                liquidity_status="locked",
                is_honeypot=False,
                risk_score=12.0,
                risk_level="low",
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.PASS
    assert assessment.mint_authority_check == CheckResult.PASS
    assert assessment.freeze_authority_check == CheckResult.PASS
    assert assessment.honeypot_check == CheckResult.PASS
    assert assessment.token is not None
    assert assessment.token.top10_holder_pct == 30.0
    assert assessment.token.mint_authority_revoked is True
    assert assessment.token.freeze_authority_revoked is True
    diagnostics = scorer.diagnostics()
    assert diagnostics["rugcheck_used"] == 1
    assert diagnostics["rugcheck_used_top_holder_pct"] == 1
    assert diagnostics["rugcheck_used_honeypot_pass"] == 1
    assert diagnostics["rugcheck_risk_level_low"] == 1
    assert signal.payload["holder_diagnostics"]["top10_holder_source"] == "rugcheck"
    assert signal.payload["holder_diagnostics"]["selected_top10_holder_pct"] == 30.0


def test_rugcheck_unsafe_data_fails_existing_checks_with_current_labels() -> None:
    mint_address = "11111111111111111111111111111111"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=False,
                freeze_authority_revoked=True,
                top_holder_pct=91.0,
                is_honeypot=True,
                risk_level="high",
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.FAIL
    assert assessment.mint_authority_check == CheckResult.FAIL
    assert assessment.honeypot_check == CheckResult.FAIL
    assert "top10_holder_check failed" in assessment.reasons
    assert "mint_authority_check failed" in assessment.reasons
    assert "honeypot_check failed" in assessment.reasons


def test_signal_creator_holding_alias_below_threshold_passes_and_sets_source() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="creator-pass-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "deployerHoldingPct": 7.5,
            "top10HolderPct": 30.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(config=RiskConfig(min_age_minutes=0), enable_holder_lookup=False)

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.creator_holding_check == CheckResult.PASS
    assert assessment.token is not None
    assert assessment.token.creator_holding_pct == 7.5
    diagnostics = signal.payload["creator_diagnostics"]
    assert diagnostics["creator_holding_pct"] == 7.5
    assert diagnostics["creator_holding_source"] == "signal_payload"
    assert diagnostics["creator_holding_state"] == "known"


def test_signal_creator_holding_alias_above_threshold_fails() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="creator-fail-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "creatorBalancePct": 18.0,
            "top10HolderPct": 30.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(config=RiskConfig(min_age_minutes=0), enable_holder_lookup=False)

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.creator_holding_check == CheckResult.FAIL
    assert assessment.token is not None
    assert assessment.token.creator_holding_pct == 18.0
    diagnostics = signal.payload["creator_diagnostics"]
    assert diagnostics["creator_holding_source"] == "signal_payload"


def test_rugcheck_raw_creator_holding_populates_when_signal_missing() -> None:
    mint_address = "So11111111111111111111111111111111111111112"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "top10HolderPct": 30.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    rugcheck = RugCheckResult(
        mint_address=mint_address,
        found=True,
        mint_authority_revoked=True,
        freeze_authority_revoked=True,
        top_holder_pct=30.0,
        provider_status="ok",
    )
    rugcheck.raw = {"creatorHoldingPct": 9.0}
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=FakeRugCheckClient(rugcheck),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.creator_holding_check == CheckResult.PASS
    assert assessment.token is not None
    assert assessment.token.creator_holding_pct == 9.0
    diagnostics = signal.payload["creator_diagnostics"]
    assert diagnostics["creator_holding_pct"] == 9.0
    assert diagnostics["creator_holding_source"] == "rugcheck"
    assert diagnostics["creator_holding_state"] == "known"


def test_creator_holding_missing_stays_unknown_and_reason_is_recorded() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="creator-unknown-mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "top10HolderPct": 30.0,
            "mintAuthorityRevoked": True,
            "freezeAuthorityRevoked": True,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(config=RiskConfig(min_age_minutes=0), enable_holder_lookup=False)

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.creator_holding_check == CheckResult.UNKNOWN
    diagnostics = signal.payload["creator_diagnostics"]
    assert diagnostics["creator_holding_pct"] is None
    assert diagnostics["creator_holding_source"] == "unknown"
    assert diagnostics["creator_holding_state"] == "unknown"
    assert diagnostics["creator_holding_unknown_reason"] == "no normalized creator holding in signal payload, RugCheck, or holder lookup"


def test_rugcheck_failing_holder_uses_local_filtered_override_when_local_passes() -> None:
    mint_address = "44444444444444444444444444444444"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=30.0)),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=91.0,
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.PASS
    assert assessment.token is not None
    assert assessment.token.top10_holder_pct == 30.0
    diagnostics = signal.payload["holder_diagnostics"]
    assert diagnostics["rugcheck_top10_holder_pct"] == 91.0
    assert diagnostics["local_filtered_top10_holder_pct"] == 30.0
    assert diagnostics["selected_top10_holder_pct"] == 30.0
    assert diagnostics["top10_holder_source"] == "local_filtered_override"
    assert scorer.diagnostics()["holder_lookup_local_override_succeeded"] == 1


def test_rugcheck_failing_holder_keeps_local_override_when_local_still_fails() -> None:
    mint_address = "55555555555555555555555555555555"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=75.0)),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=120.0,
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.FAIL
    assert assessment.token is not None
    assert assessment.token.top10_holder_pct == 75.0
    diagnostics = signal.payload["holder_diagnostics"]
    assert diagnostics["rugcheck_top10_holder_pct"] == 120.0
    assert diagnostics["local_filtered_top10_holder_pct"] == 75.0
    assert diagnostics["selected_top10_holder_pct"] == 75.0
    assert diagnostics["top10_holder_source"] == "local_filtered_override"


def test_rugcheck_failing_holder_stays_conservative_when_local_lookup_unavailable() -> None:
    mint_address = "66666666666666666666666666666666"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(error=RuntimeError("provider boom")),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=91.0,
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.FAIL
    assert assessment.token is not None
    assert assessment.token.top10_holder_pct == 91.0
    diagnostics = signal.payload["holder_diagnostics"]
    assert diagnostics["rugcheck_top10_holder_pct"] == 91.0
    assert diagnostics["local_filtered_top10_holder_pct"] is None
    assert diagnostics["selected_top10_holder_pct"] == 91.0
    assert diagnostics["top10_holder_source"] == "rugcheck_no_local_override"


def test_rugcheck_missing_holder_keeps_existing_local_lookup_behavior() -> None:
    mint_address = "77777777777777777777777777777777"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=30.0)),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=None,
                provider_status="ok",
            )
        ),
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.PASS
    assert assessment.token is not None
    assert assessment.token.top10_holder_pct == 30.0
    diagnostics = signal.payload["holder_diagnostics"]
    assert diagnostics["rugcheck_top10_holder_pct"] is None
    assert diagnostics["local_filtered_top10_holder_pct"] == 30.0
    assert diagnostics["selected_top10_holder_pct"] == 30.0
    assert diagnostics["top10_holder_source"] == "local_filtered_lookup"


def test_rugcheck_unavailable_falls_back_to_existing_behavior() -> None:
    mint_address = "22222222222222222222222222222222"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "creatorHoldingPct": 5.0,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        holder_lookup=FakeHolderLookup(HolderLookupResult(top10_holder_pct=30.0)),
        rugcheck_client=FakeRugCheckClient(error=RuntimeError("provider boom")),
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.top10_holder_check == CheckResult.PASS
    assert assessment.creator_holding_check == CheckResult.PASS
    assert assessment.mint_authority_check == CheckResult.UNKNOWN
    assert assessment.freeze_authority_check == CheckResult.UNKNOWN
    assert assessment.honeypot_check == CheckResult.UNKNOWN
    diagnostics = scorer.diagnostics()
    assert diagnostics["holder_lookup_succeeded"] == 1
    assert diagnostics["rugcheck_failed_provider_error"] == 1


def test_strict_thresholds_are_unchanged_when_rugcheck_adds_safe_metadata() -> None:
    mint_address = "33333333333333333333333333333333"
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": datetime.now(UTC).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=False,
                provider_status="ok",
            )
        ),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.age_check == CheckResult.FAIL


def test_funding_analysis_shared_funder_majority_fails_buyer_gate() -> None:
    as_of = datetime.now(UTC) - timedelta(minutes=10)
    buyers = [f"buyer-{index}" for index in range(40)]
    transfers_by_wallet = {
        wallet: [InboundTransfer(source_wallet="shared-funder", observed_at=as_of)]
        for wallet in buyers[:35]
    }
    for wallet in buyers[35:]:
        transfers_by_wallet[wallet] = [InboundTransfer(source_wallet=f"funder-{wallet}", observed_at=as_of)]

    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="So11111111111111111111111111111111111111112",
        payload={
            "vSolInBondingCurve": 30.1,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            "buyerWallets": buyers,
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        funding_provider=FakeFundingProvider(transfers_by_wallet),
        enable_holder_lookup=False,
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=signal.mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=False,
                provider_status="ok",
            )
        ),
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.unique_buyers_check == CheckResult.FAIL
    assert assessment.token is not None
    assert assessment.token.unique_buyers == 40
    diagnostics = scorer.diagnostics()
    assert diagnostics["funding_analysis_used"] == 1
    assert diagnostics["funding_analysis_failed_threshold"] == 1


def test_funding_analysis_diverse_funders_pass_and_preserve_existing_thresholds() -> None:
    as_of = datetime.now(UTC) - timedelta(minutes=10)
    buyers = [f"buyer-{index}" for index in range(8)]
    transfers_by_wallet = {
        wallet: [InboundTransfer(source_wallet=f"funder-{index}", observed_at=as_of)]
        for index, wallet in enumerate(buyers)
    }
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="11111111111111111111111111111111",
        payload={
            "vSolInBondingCurve": 30.1,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            "buyerWallets": buyers,
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        funding_provider=FakeFundingProvider(transfers_by_wallet),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.unique_buyers_check == CheckResult.FAIL
    diagnostics = scorer.diagnostics()
    assert diagnostics["funding_analysis_used"] == 1
    assert diagnostics["funding_analysis_passed"] == 1


def test_funding_analysis_missing_buyer_wallets_degrades_safely() -> None:
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="22222222222222222222222222222222",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "buyerWallets": [],
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        funding_provider=FakeFundingProvider({}),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.unique_buyers_check == CheckResult.PASS
    assert scorer.diagnostics()["funding_analysis_missing_buyers"] == 1


def test_funding_analysis_missing_provider_does_not_crash_and_stays_conservative() -> None:
    buyers = [f"buyer-{index}" for index in range(25)]
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="33333333333333333333333333333333",
        payload={
            "vSolInBondingCurve": 30.1,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            "buyerWallets": buyers,
        },
    )
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        funding_provider=MissingProviderFundingProvider(),
        enable_holder_lookup=False,
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.unique_buyers_check == CheckResult.UNKNOWN
    diagnostics = scorer.diagnostics()
    assert diagnostics["funding_analysis_used"] == 1
    assert diagnostics["funding_analysis_missing_provider"] == 1
    assert diagnostics["funding_analysis_unknown"] == 1


def test_parallel_prechecks_preserve_results_for_safe_input() -> None:
    mint_address = "So11111111111111111111111111111111111111112"
    buyers = [f"buyer-{index}" for index in range(2)]
    as_of = datetime.now(UTC) - timedelta(minutes=10)
    transfers_by_wallet = {
        wallet: [InboundTransfer(source_wallet=f"funder-{index}", observed_at=as_of)]
        for index, wallet in enumerate(buyers)
    }
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address,
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": as_of.isoformat(),
            "buyerWallets": buyers,
        },
    )
    expected_scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=False,
                provider_status="ok",
            )
        ),
        funding_provider=FakeFundingProvider(transfers_by_wallet),
        enable_holder_lookup=False,
    )
    parallel_scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=SlowRugCheckClient(
            0.1,
            RugCheckResult(
                mint_address=mint_address,
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=False,
                provider_status="ok",
            ),
        ),
        funding_provider=SlowFundingProvider(0.1, transfers_by_wallet),
        enable_holder_lookup=False,
    )

    expected = asyncio.run(expected_scorer.assess_signal(signal))
    started_at = monotonic()
    actual = asyncio.run(parallel_scorer.assess_signal(signal))
    elapsed = monotonic() - started_at

    assert actual.score == expected.score
    assert actual.reasons == expected.reasons
    assert actual.honeypot_check == expected.honeypot_check
    assert actual.unique_buyers_check == expected.unique_buyers_check
    assert actual.top10_holder_check == expected.top10_holder_check
    assert elapsed < 0.28
    assert parallel_scorer.diagnostics()["parallel_prechecks_used"] == 1


def test_honeypot_simulation_blocked_result_fails_existing_honeypot_check() -> None:
    provider = RecordingSimulationProvider({"success": False, "blockedReason": "sell blocked by program"})
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address="mint",
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=None,
                provider_status="ok",
            )
        ),
        honeypot_adapter=HoneypotSimulationAdapter(provider=provider),
        enable_holder_lookup=False,
        enable_funding_analysis=False,
    )
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            "sellTransactionPayload": "serialized-sell",
        },
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.honeypot_check == CheckResult.FAIL
    assert "honeypot_check failed" in assessment.reasons
    diagnostics = scorer.diagnostics()
    assert diagnostics["honeypot_simulation_used"] == 1
    assert diagnostics["honeypot_simulation_blocked"] == 1
    assert len(provider.calls) == 1


def test_honeypot_simulation_unavailable_falls_back_safely() -> None:
    provider = RecordingSimulationProvider(RuntimeError("boom"))
    scorer = DiscoveryRiskScorer(
        config=RiskConfig(min_age_minutes=0),
        rugcheck_client=FakeRugCheckClient(
            RugCheckResult(
                mint_address="mint",
                found=True,
                mint_authority_revoked=True,
                freeze_authority_revoked=True,
                top_holder_pct=30.0,
                is_honeypot=None,
                provider_status="ok",
            )
        ),
        honeypot_adapter=HoneypotSimulationAdapter(provider=provider),
        enable_holder_lookup=False,
        enable_funding_analysis=False,
    )
    signal = Signal(
        source=SignalSource.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="mint",
        payload={
            "vSolInBondingCurve": 30.1,
            "uniqueBuyers": 25,
            "createdAt": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            "sellTransactionPayload": "serialized-sell",
        },
    )

    assessment = asyncio.run(scorer.assess_signal(signal))

    assert assessment.honeypot_check == CheckResult.UNKNOWN
    diagnostics = scorer.diagnostics()
    assert diagnostics["honeypot_simulation_used"] == 1
    assert diagnostics["honeypot_simulation_provider_error"] == 1
