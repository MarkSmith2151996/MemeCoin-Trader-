from datetime import UTC, datetime, timedelta

from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.risk.scorer import assess_signal, assess_token, build_token_from_signal


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
