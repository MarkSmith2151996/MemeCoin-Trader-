import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import src.cli as cli_module
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource as SignalSourceEnum, SignalType, TokenInfo
from src.signals.base import SignalSource


class FakeSignalSource(SignalSource):
    def __init__(self, batches: list[list[Signal]], *, name: str = "fake") -> None:
        self._batches = list(batches)
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def poll(self) -> list[Signal]:
        if self._batches:
            return self._batches.pop(0)
        return []


def _build_rejected_signal() -> Signal:
    assessment = RiskAssessment(
        token=TokenInfo(
            mint_address="RejectedMint1111111111111111111111111111",
            symbol="REKT",
            name="Rejected Token",
            liquidity_sol=4.0,
            top10_holder_pct=72.5,
            creator_holding_pct=5.0,
            unique_buyers=30,
            mint_authority_revoked=True,
            freeze_authority_revoked=True,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
            market_cap_usd=12345.0,
        ),
        liquidity_check=CheckResult.FAIL,
        top10_holder_check=CheckResult.FAIL,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.UNKNOWN,
        score=55.0,
        reasons=["liquidity_check failed", "top10_holder_check failed"],
    )
    return Signal(
        source=SignalSourceEnum.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="RejectedMint1111111111111111111111111111",
        confidence=0.85,
        weight=1.0,
        payload={
            "symbol": "REKT",
            "name": "Rejected Token",
            "marketCapUsd": 12345.0,
            "metrics": {"volume_m5": 321.0, "buys_m5": 8, "sells_m5": 3},
            "social_credibility": {"highest_tier": "medium", "unique_accounts": 2},
            "holder_diagnostics": {
                "rugcheck_top10_holder_pct": 91.0,
                "local_filtered_top10_holder_pct": 42.0,
                "selected_top10_holder_pct": 42.0,
                "top10_holder_source": "local_filtered_override",
                "bonding_curve_addresses": ["BondingCurve111111111111111111111111111111"],
                "local_holder_raw_account_count": 2,
                "local_holder_filtered_account_count": 1,
                "local_holder_retained_account_count": 1,
                "local_holder_top_filtered_accounts": [{"address": "BondingCurve111111111111111111111111111111", "classification": "bonding_curve_artifact"}],
                "local_holder_top_retained_accounts": [{"address": "Holder11111111111111111111111111111111111", "classification": "retained"}],
            },
            "creator_diagnostics": {
                "creator_holding_pct": None,
                "creator_holding_source": "unknown",
                "creator_holding_state": "unknown",
                "creator_holding_unknown_reason": "no normalized creator holding in signal payload, RugCheck, or holder lookup",
            },
            "holder_policy": {
                "holder_policy_state": "fresh_launch_warning",
                "holder_policy_reason": "discovery-mode fresh-launch concentration warning with trusted holder source and clean supporting checks",
                "token_age_minutes": 10.0,
                "stage_hint": "new_pool",
                "fresh_launch_context_used": True,
            },
            "age_policy": {
                "age_policy_state": "immature_warning",
                "age_policy_reason": "discovery-mode seconds-old launch allowed past age as warning because other hard safety checks were not failing",
                "token_age_minutes": 0.2,
                "stage_hint": "new_pool",
                "age_policy_context_used": True,
            },
            "liquidity_diagnostics": {
                "selected_liquidity_sol": 4.0,
                "selected_liquidity_usd": 500.0,
                "liquidity_source": "jupiter_fallback",
                "liquidity_data_state": "known",
                "liquidity_unknown_reason": None,
                "dexscreener_liquidity_sol": None,
                "dexscreener_liquidity_usd": None,
                "dexscreener_status": "no_solana_liquidity",
                "jupiter_liquidity_sol": 4.0,
                "jupiter_liquidity_usd": None,
                "jupiter_status": "ok",
                "fallback_attempted": True,
                "fallback_succeeded": True,
            },
            "raw_data": {"secret": "do-not-print", "buyerWallets": ["WalletSecret111"]},
            "risk_assessment": assessment,
        },
    )


def test_run_bounded_paper_cycle_collects_per_token_rejection_diagnostics(tmp_path: Path) -> None:
    async def run() -> None:
        db_path = tmp_path / "diagnostics.db"
        source = FakeSignalSource([[_build_rejected_signal()]])

        summary = await cli_module.run_bounded_paper_cycle(
            max_signals=1,
            timeout_seconds=0.1,
            db_path=db_path,
            sources=[source],
            poll_interval_s=0.0,
        )

        assert summary.signals_collected == 1
        assert summary.signals_rejected == 1
        assert len(summary.rejected_candidate_diagnostics) == 1
        diagnostic = summary.rejected_candidate_diagnostics[0]
        assert diagnostic["symbol"] == "REKT"
        assert diagnostic["failed_check"] == "liquidity_check"
        assert diagnostic["rugcheck_top10_holder_pct"] == 91.0
        assert diagnostic["local_filtered_top10_holder_pct"] == 42.0
        assert diagnostic["selected_top10_holder_pct"] == 42.0
        assert diagnostic["top10_holder_source"] == "local_filtered_override"
        assert diagnostic["top10_holder_pct"] == 42.0
        assert diagnostic["local_holder_filtered_account_count"] == 1
        assert diagnostic["local_holder_top_filtered_accounts"][0]["classification"] == "bonding_curve_artifact"
        assert diagnostic["creator_holding_source"] == "unknown"
        assert diagnostic["creator_holding_state"] == "unknown"
        assert diagnostic["holder_policy_state"] == "fresh_launch_warning"
        assert diagnostic["age_policy_state"] == "immature_warning"
        assert diagnostic["selected_liquidity_sol"] == 4.0
        assert diagnostic["selected_liquidity_usd"] == 500.0
        assert diagnostic["liquidity_source"] == "jupiter_fallback"
        assert diagnostic["liquidity_data_state"] == "known"
        assert diagnostic["fallback_attempted"] is True
        assert diagnostic["fallback_succeeded"] is True
        assert diagnostic["liquidity_state"] == "fail"
        assert diagnostic["liquidity_display"] == "4.0000"
        assert "mc=12345.00" in diagnostic["attention_hints"]

    asyncio.run(run())


def test_rejection_report_and_cli_lines_stay_safe_with_missing_fields() -> None:
    summary = cli_module.PaperCycleSummary(
        execution_mode="paper",
        risk_profile="strict",
        max_signals=1,
        timeout_seconds=60.0,
        signals_collected=1,
        signals_accepted=0,
        signals_rejected=1,
        trades_persisted=0,
        open_positions=0,
        sources_polled=["pump_fun"],
        source_signal_counts={"pump_fun": 1},
        source_failures={},
        composite_opportunities=0,
        rejection_reasons={"liquidity_check_failed": 1},
        candidates_evaluated=1,
        passed_risk_checks=0,
        summary_rejection_reasons={"liquidity": 1},
        source_evaluated_counts={"pump_fun": 1},
        source_pass_counts={"pump_fun": 0},
        holder_lookup_outcomes={},
        termination_reason="max_signals",
        elapsed_seconds=1.0,
        rejected_candidate_diagnostics=[
            {
                "rank": 1,
                "symbol": "REKT",
                "mint_short": "Reje...1111",
                "mint": "RejectedMint1111111111111111111111111111",
                "source": "pump_fun",
                "failed_check": "liquidity_check",
                "liquidity_display": "unknown",
                "attention_hints": "none",
                "notes": "missing fields handled",
            }
        ],
    )

    cli_lines = summary.rejection_diagnostic_lines()
    report = cli_module.build_rejection_diagnostic_report(summary)

    assert cli_lines[0] == "Rejected candidate diagnostics:"
    assert "failed_check" in cli_lines[1]
    assert "holder_policy" in cli_lines[1]
    assert "age_policy" in cli_lines[1]
    assert "holder_source" in cli_lines[1]
    assert "creator" in cli_lines[1]
    assert "liquidity_check" in cli_lines[2]
    assert "creator_holding_unknown_reason" in report
    assert "age_policy_reason" in report
    assert "holder_policy_reason" in report
    assert "liquidity_source" in report
    assert "liquidity_data_state" in report
    assert "fallback_attempted" in report
    assert "local_holder_top_filtered_accounts" in report
    assert "do-not-print" not in report
    assert "WalletSecret111" not in report
    assert "raw_data" not in report
