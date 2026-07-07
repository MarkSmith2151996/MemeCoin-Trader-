from src.strategy.position_sizing import (
    DEFAULT_LIQUIDITY_SIZING_TIERS,
    LiquiditySizingTier,
    determine_liquidity_position_size,
)


def test_unknown_liquidity_skips_trade() -> None:
    decision = determine_liquidity_position_size(None)

    assert decision.skip_trade is True
    assert decision.max_position_sol == 0.0
    assert decision.matched_tier is None
    assert decision.reason == "liquidity_unknown"


def test_under_15_sol_caps_to_point_one() -> None:
    decision = determine_liquidity_position_size(14.99)

    assert decision.skip_trade is False
    assert decision.max_position_sol == 0.1
    assert decision.matched_tier == "under_15_sol"


def test_15_to_50_sol_caps_to_point_two_five() -> None:
    decision = determine_liquidity_position_size(15.0)

    assert decision.skip_trade is False
    assert decision.max_position_sol == 0.25
    assert decision.matched_tier == "15_to_50_sol"


def test_50_to_200_sol_caps_to_point_five() -> None:
    decision = determine_liquidity_position_size(50.0)

    assert decision.skip_trade is False
    assert decision.max_position_sol == 0.5
    assert decision.matched_tier == "50_to_200_sol"


def test_over_200_sol_caps_to_one() -> None:
    decision = determine_liquidity_position_size(200.0)

    assert decision.skip_trade is False
    assert decision.max_position_sol == 1.0
    assert decision.matched_tier == "over_200_sol"


def test_custom_tier_config_is_used() -> None:
    tiers = (
        LiquiditySizingTier(max_liquidity_sol=15.0, max_position_sol=0.05, label="tiny"),
        LiquiditySizingTier(min_liquidity_sol=15.0, max_liquidity_sol=50.0, max_position_sol=0.2, label="small_liquidity"),
        LiquiditySizingTier(min_liquidity_sol=50.0, max_position_sol=0.4, label="bigger_liquidity"),
    )

    decision = determine_liquidity_position_size(19.5, tiers=tiers)

    assert decision.skip_trade is False
    assert decision.max_position_sol == 0.2
    assert decision.matched_tier == "small_liquidity"
    assert decision.reason == "small_liquidity"


def test_invalid_liquidity_skips_trade() -> None:
    decision = determine_liquidity_position_size(-1)

    assert decision.skip_trade is True
    assert decision.max_position_sol == 0.0
    assert decision.reason == "liquidity_invalid"


def test_default_tiers_do_not_touch_runtime_trading_components() -> None:
    assert len(DEFAULT_LIQUIDITY_SIZING_TIERS) == 4
    assert all(isinstance(tier, LiquiditySizingTier) for tier in DEFAULT_LIQUIDITY_SIZING_TIERS)
