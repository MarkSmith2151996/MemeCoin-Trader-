from src.strategy.market_regime import (
    HOT,
    RISKY,
    THIN,
    UNKNOWN,
    MarketRegimeInputs,
    detect_market_regime,
)


def test_high_signal_count_and_healthy_liquidity_classifies_hot() -> None:
    result = detect_market_regime(
        MarketRegimeInputs(
            new_pool_count=12,
            average_liquidity_sol=95.0,
            median_volume_sol=180.0,
            median_transaction_count=140,
            paper_trade_success_rate=0.7,
            paper_trade_sample_size=10,
            signal_count=14,
            signal_velocity_per_hour=6.5,
        )
    )

    assert result.regime == HOT
    assert result.confidence == 0.9
    assert result.reason_labels == (
        "high_signal_count",
        "high_signal_velocity",
        "healthy_liquidity",
        "healthy_flow",
    )
    assert result.adjustment_hints.position_cap_multiplier == 1.0
    assert result.adjustment_hints.risk_appetite == "measured"


def test_low_liquidity_and_failed_trades_classifies_risky() -> None:
    result = detect_market_regime(
        MarketRegimeInputs(
            new_pool_count=7,
            average_liquidity_sol=12.0,
            median_volume_sol=45.0,
            median_transaction_count=40,
            paper_trade_success_rate=0.2,
            paper_trade_sample_size=8,
            signal_count=9,
            signal_velocity_per_hour=4.0,
        )
    )

    assert result.regime == RISKY
    assert result.confidence == 0.9
    assert result.reason_labels == (
        "low_average_liquidity",
        "paper_trades_failing",
        "high_signal_count",
    )
    assert result.adjustment_hints.position_cap_multiplier == 0.35
    assert result.adjustment_hints.signal_threshold_multiplier == 1.35


def test_low_activity_classifies_thin() -> None:
    result = detect_market_regime(
        MarketRegimeInputs(
            new_pool_count=1,
            average_liquidity_sol=18.0,
            median_volume_sol=12.0,
            median_transaction_count=10,
            signal_count=2,
            signal_velocity_per_hour=0.5,
        )
    )

    assert result.regime == THIN
    assert result.confidence == 0.8
    assert result.reason_labels == (
        "low_average_liquidity",
        "low_new_pool_activity",
        "low_signal_activity",
        "low_transaction_flow",
    )
    assert result.adjustment_hints.position_cap_multiplier == 0.6
    assert result.adjustment_hints.risk_appetite == "cautious"


def test_missing_data_returns_unknown_safely() -> None:
    result = detect_market_regime(MarketRegimeInputs())

    assert result.regime == UNKNOWN
    assert result.confidence == 0.2
    assert result.reason_labels == ("insufficient_activity_data",)
    assert result.input_summary == {
        "new_pool_count": None,
        "average_liquidity_sol": None,
        "median_volume_sol": None,
        "median_transaction_count": None,
        "paper_trade_success_rate": None,
        "paper_trade_sample_size": None,
        "signal_count": None,
        "signal_velocity_per_hour": None,
    }


def test_confidence_and_reason_labels_are_deterministic() -> None:
    inputs = MarketRegimeInputs(
        new_pool_count=4,
        average_liquidity_sol=55.0,
        median_volume_sol=70.0,
        median_transaction_count=60,
        paper_trade_success_rate=0.5,
        paper_trade_sample_size=6,
        signal_count=5,
        signal_velocity_per_hour=2.0,
    )

    first = detect_market_regime(inputs)
    second = detect_market_regime(inputs)

    assert first == second
    assert first.regime == "normal"
    assert first.confidence == 0.65
    assert first.reason_labels == ()


def test_market_regime_helper_has_no_runtime_integration_side_effects() -> None:
    inputs = MarketRegimeInputs(
        new_pool_count=2,
        average_liquidity_sol=20.0,
        signal_count=1,
        signal_velocity_per_hour=0.2,
    )

    before = inputs
    result = detect_market_regime(inputs)

    assert inputs == before
    assert result.input_summary["new_pool_count"] == 2
    assert result.adjustment_hints.risk_appetite in {"cautious", "balanced", "minimal", "measured", "defensive"}
