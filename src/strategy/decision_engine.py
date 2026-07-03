"""Risk-gated decision helpers."""

from __future__ import annotations

from dataclasses import dataclass

from src.core.models import RiskAssessment, Signal


@dataclass(frozen=True)
class TradeDecision:
    should_buy: bool
    reason: str
    confidence: float


def evaluate_buy_signal(signal: Signal, risk: RiskAssessment, min_confidence: float = 0.6) -> TradeDecision:
    if not risk.all_checks_pass:
        return TradeDecision(False, "risk checks did not all pass", signal.confidence)
    if signal.confidence < min_confidence:
        return TradeDecision(False, "signal confidence below threshold", signal.confidence)
    return TradeDecision(True, "risk and confidence checks passed", signal.confidence)
