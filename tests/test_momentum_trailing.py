from src.strategy.momentum_trailing import (
    HARD_STOP_PCT,
    TIGHTENED_TRAIL_ACTIVATION_PCT,
    TIGHTENED_TRAIL_PCT,
    MomentumTrailState,
    evaluate_momentum_trail,
)


def test_hard_stop_closes_at_twenty_percent_loss() -> None:
    decision = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 0.79)
    assert decision.exit_reason == "hard_stop_loss"
    assert HARD_STOP_PCT == -0.20


def test_standard_trailing_stop_uses_eight_percent_drawdown() -> None:
    armed = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 1.10)
    assert armed.state.trail_activated is True
    assert armed.exit_reason is None
    exited = evaluate_momentum_trail(armed.state, 0.99)
    assert exited.exit_reason == "trailing_stop"
    # 0.99 is 1% below 1.0 entry — hard stop would not fire at -20%


def test_tightened_trailing_stop_uses_five_percent_drawdown_after_twenty_five_percent_peak() -> None:
    armed = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 1.3)
    assert armed.state.peak_price_sol == 1.3
    assert TIGHTENED_TRAIL_ACTIVATION_PCT == 0.25
    assert TIGHTENED_TRAIL_PCT == 0.05
    exited = evaluate_momentum_trail(armed.state, 1.23)
    assert exited.exit_reason == "trailing_stop_tight"
