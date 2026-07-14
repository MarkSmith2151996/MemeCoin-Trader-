from src.strategy.momentum_trailing import MomentumTrailState, evaluate_momentum_trail


def test_hard_stop_closes_at_twenty_five_percent_loss() -> None:
    decision = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 0.75)
    assert decision.exit_reason == "hard_stop_loss"


def test_standard_trailing_stop_arms_at_ten_percent_profit() -> None:
    armed = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 1.10)
    assert armed.state.trail_activated is True
    assert armed.exit_reason is None
    exited = evaluate_momentum_trail(armed.state, 0.99)
    assert exited.exit_reason == "trailing_stop"


def test_tightened_trailing_stop_uses_ten_percent_drawdown_after_fifty_percent_peak() -> None:
    armed = evaluate_momentum_trail(MomentumTrailState(1.0, 1.0), 1.5)
    assert armed.state.peak_price_sol == 1.5
    exited = evaluate_momentum_trail(armed.state, 1.35)
    assert exited.exit_reason == "trailing_stop_tight"
