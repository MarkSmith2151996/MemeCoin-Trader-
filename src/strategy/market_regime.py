"""Pure helper logic for classifying aggregate meme-market conditions."""

from __future__ import annotations

from dataclasses import dataclass, field


HOT = "hot"
NORMAL = "normal"
THIN = "thin"
RISKY = "risky"
UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class MarketRegimeInputs:
    new_pool_count: int | None = None
    average_liquidity_sol: float | None = None
    median_volume_sol: float | None = None
    median_transaction_count: float | None = None
    paper_trade_success_rate: float | None = None
    paper_trade_sample_size: int | None = None
    signal_count: int | None = None
    signal_velocity_per_hour: float | None = None


@dataclass(frozen=True, slots=True)
class MarketRegimeAdjustmentHints:
    position_cap_multiplier: float
    signal_threshold_multiplier: float
    risk_appetite: str


@dataclass(frozen=True, slots=True)
class MarketRegimeResult:
    regime: str
    confidence: float
    reason_labels: tuple[str, ...] = ()
    input_summary: dict[str, float | int | None] = field(default_factory=dict)
    adjustment_hints: MarketRegimeAdjustmentHints = field(
        default_factory=lambda: MarketRegimeAdjustmentHints(
            position_cap_multiplier=0.5,
            signal_threshold_multiplier=1.25,
            risk_appetite="minimal",
        )
    )


def detect_market_regime(inputs: MarketRegimeInputs) -> MarketRegimeResult:
    summary = {
        "new_pool_count": _normalized_int(inputs.new_pool_count),
        "average_liquidity_sol": _normalized_float(inputs.average_liquidity_sol),
        "median_volume_sol": _normalized_float(inputs.median_volume_sol),
        "median_transaction_count": _normalized_float(inputs.median_transaction_count),
        "paper_trade_success_rate": _normalized_ratio(inputs.paper_trade_success_rate),
        "paper_trade_sample_size": _normalized_int(inputs.paper_trade_sample_size),
        "signal_count": _normalized_int(inputs.signal_count),
        "signal_velocity_per_hour": _normalized_float(inputs.signal_velocity_per_hour),
    }
    reasons: list[str] = []

    has_activity_data = any(
        summary[key] is not None
        for key in (
            "new_pool_count",
            "average_liquidity_sol",
            "median_volume_sol",
            "median_transaction_count",
            "signal_count",
            "signal_velocity_per_hour",
        )
    )
    if not has_activity_data:
        return MarketRegimeResult(
            regime=UNKNOWN,
            confidence=0.2,
            reason_labels=("insufficient_activity_data",),
            input_summary=summary,
            adjustment_hints=_hints_for_regime(UNKNOWN),
        )

    new_pool_count = summary["new_pool_count"] or 0
    average_liquidity_sol = summary["average_liquidity_sol"] or 0.0
    median_volume_sol = summary["median_volume_sol"] or 0.0
    median_transaction_count = summary["median_transaction_count"] or 0.0
    success_rate = summary["paper_trade_success_rate"]
    trade_sample_size = summary["paper_trade_sample_size"] or 0
    signal_count = summary["signal_count"] or 0
    signal_velocity = summary["signal_velocity_per_hour"] or 0.0

    failure_rate = None if success_rate is None else round(1.0 - success_rate, 4)

    if average_liquidity_sol > 0 and average_liquidity_sol < 20.0:
        reasons.append("low_average_liquidity")
    if trade_sample_size >= 4 and failure_rate is not None and failure_rate >= 0.6:
        reasons.append("paper_trades_failing")
    if signal_count >= 8:
        reasons.append("high_signal_count")
    if signal_velocity >= 5.0:
        reasons.append("high_signal_velocity")
    if average_liquidity_sol >= 75.0:
        reasons.append("healthy_liquidity")
    if median_transaction_count >= 120 or median_volume_sol >= 150.0:
        reasons.append("healthy_flow")
    if new_pool_count <= 2:
        reasons.append("low_new_pool_activity")
    if signal_count <= 3 and signal_velocity <= 1.0:
        reasons.append("low_signal_activity")
    if median_transaction_count > 0 and median_transaction_count < 25:
        reasons.append("low_transaction_flow")

    if "low_average_liquidity" in reasons and "paper_trades_failing" in reasons:
        regime = RISKY
    elif (
        new_pool_count >= 8
        and average_liquidity_sol >= 75.0
        and signal_count >= 8
        and signal_velocity >= 5.0
        and (median_transaction_count >= 120 or median_volume_sol >= 150.0)
        and not (trade_sample_size >= 4 and failure_rate is not None and failure_rate >= 0.4)
    ):
        regime = HOT
    elif new_pool_count <= 2 and signal_count <= 3 and signal_velocity <= 1.0 and average_liquidity_sol < 40.0:
        regime = THIN
    else:
        regime = NORMAL

    return MarketRegimeResult(
        regime=regime,
        confidence=_confidence_for_regime(regime),
        reason_labels=tuple(reasons),
        input_summary=summary,
        adjustment_hints=_hints_for_regime(regime),
    )


def _hints_for_regime(regime: str) -> MarketRegimeAdjustmentHints:
    if regime == HOT:
        return MarketRegimeAdjustmentHints(
            position_cap_multiplier=1.0,
            signal_threshold_multiplier=0.9,
            risk_appetite="measured",
        )
    if regime == NORMAL:
        return MarketRegimeAdjustmentHints(
            position_cap_multiplier=0.85,
            signal_threshold_multiplier=1.0,
            risk_appetite="balanced",
        )
    if regime == THIN:
        return MarketRegimeAdjustmentHints(
            position_cap_multiplier=0.6,
            signal_threshold_multiplier=1.2,
            risk_appetite="cautious",
        )
    if regime == RISKY:
        return MarketRegimeAdjustmentHints(
            position_cap_multiplier=0.35,
            signal_threshold_multiplier=1.35,
            risk_appetite="defensive",
        )
    return MarketRegimeAdjustmentHints(
        position_cap_multiplier=0.5,
        signal_threshold_multiplier=1.25,
        risk_appetite="minimal",
    )


def _confidence_for_regime(regime: str) -> float:
    if regime == HOT:
        return 0.9
    if regime == NORMAL:
        return 0.65
    if regime == THIN:
        return 0.8
    if regime == RISKY:
        return 0.9
    return 0.2


def _normalized_int(value: int | None) -> int | None:
    if value is None:
        return None
    return max(int(value), 0)


def _normalized_float(value: float | None) -> float | None:
    if value is None:
        return None
    return round(max(float(value), 0.0), 6)


def _normalized_ratio(value: float | None) -> float | None:
    if value is None:
        return None
    return round(min(max(float(value), 0.0), 1.0), 6)
