from datetime import date, timedelta

from src.execution.live_daily_caps import DailyLiveState, evaluate_daily_live_caps


def test_daily_live_caps_allow_current_usage_below_limits() -> None:
    decision = evaluate_daily_live_caps(
        DailyLiveState(day=date.today(), submitted_trade_count=0, realized_loss_sol=0.0),
        max_daily_trades=1,
        max_daily_loss_sol=0.02,
    )

    assert decision.allowed is True
    assert decision.diagnostics == ("daily_live_caps_passed",)


def test_daily_live_caps_block_exhausted_trade_or_loss_limits() -> None:
    trade_limit = evaluate_daily_live_caps(
        DailyLiveState(day=date.today(), submitted_trade_count=1, realized_loss_sol=0.0),
        max_daily_trades=1,
        max_daily_loss_sol=0.02,
    )
    loss_limit = evaluate_daily_live_caps(
        DailyLiveState(day=date.today(), submitted_trade_count=0, realized_loss_sol=0.02),
        max_daily_trades=1,
        max_daily_loss_sol=0.02,
    )

    assert trade_limit.allowed is False
    assert trade_limit.diagnostics == ("daily_live_trade_cap_exhausted",)
    assert loss_limit.allowed is False
    assert loss_limit.diagnostics == ("daily_live_loss_cap_exhausted",)


def test_daily_live_caps_block_missing_stale_or_malformed_state() -> None:
    unavailable = evaluate_daily_live_caps(None, max_daily_trades=1, max_daily_loss_sol=0.02)
    stale = evaluate_daily_live_caps(
        DailyLiveState(day=date.today() - timedelta(days=1), submitted_trade_count=0, realized_loss_sol=0.0),
        max_daily_trades=1,
        max_daily_loss_sol=0.02,
    )
    malformed = evaluate_daily_live_caps(
        DailyLiveState(day=date.today(), submitted_trade_count=-1, realized_loss_sol=0.0),
        max_daily_trades=1,
        max_daily_loss_sol=0.02,
    )

    assert unavailable.diagnostics == ("daily_live_state_unavailable",)
    assert stale.diagnostics == ("daily_live_state_stale",)
    assert malformed.diagnostics == ("daily_live_state_malformed",)
