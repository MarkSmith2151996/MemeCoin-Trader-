"""Risk-gated trading decisions and exit execution."""

from __future__ import annotations

import inspect
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import structlog
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    structlog = None

from src.core.config import Settings
from src.core.database import record_trade
from src.core.models import CheckResult, PartialExit, Position, RiskAssessment, Side, Signal, TokenInfo, Trade
from src.execution.base import ExecutionAdapter
from src.strategy.exits import evaluate_exits
from src.strategy.market_regime import MarketRegimeInputs, detect_market_regime
from src.strategy.position_manager import PositionManager
from src.strategy.position_sizing import determine_liquidity_position_size


logger = logging.getLogger(__name__)
telemetry_logger = structlog.get_logger(__name__) if structlog is not None else logger


@dataclass(slots=True)
class DecisionResult:
    trade: Trade | None
    rejection_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class PositionSizingResult:
    amount_sol: float
    metadata: dict[str, object]
    rejection_reason: str | None = None


@dataclass(slots=True)
class RejectionRecord:
    mint_address: str
    signal_source: str
    signal_strength: float
    timestamp: datetime
    outcome: str
    failed_check: str | None
    check_results: dict[str, dict[str, object]]
    risk_score: float | None = None


class DecisionEngine:
    def __init__(
        self,
        execution: ExecutionAdapter,
        risk_scorer: Any,
        position_manager: PositionManager,
        config: Settings,
        db: str | Path | None = None,
        *,
        market_regime_enabled: bool = False,
    ) -> None:
        self.execution = execution
        self.risk_scorer = risk_scorer
        self.position_manager = position_manager
        self.config = config
        self.db = Path(db) if db is not None else None
        self.market_regime_enabled = market_regime_enabled

    async def evaluate_signal(self, signal: Signal) -> Trade | None:
        return (await self.evaluate_signal_with_diagnostics(signal)).trade

    async def evaluate_signal_with_diagnostics(self, signal: Signal) -> DecisionResult:
        if not signal.mint_address.strip():
            logger.info("Skipping signal without mint address")
            telemetry = self._build_rejection_record(signal, outcome="rejected")
            self._log_rejection_record(telemetry)
            return DecisionResult(
                trade=None,
                rejection_reason="missing_mint_address",
                metadata={"rejection_record": asdict(telemetry)},
            )

        existing = await self.position_manager.get_position(signal.mint_address)
        if existing is not None:
            logger.info("Skipping %s: open position already exists", signal.mint_address)
            telemetry = self._build_rejection_record(signal, outcome="rejected")
            self._log_rejection_record(telemetry)
            return DecisionResult(
                trade=None,
                rejection_reason="open_position_exists",
                metadata={"rejection_record": asdict(telemetry)},
            )

        regime_metadata = self._market_regime_metadata(signal)

        risk = await self._assess_signal(signal)
        rejection_record = self._build_rejection_record(
            signal,
            risk=risk,
            outcome="passed" if risk.all_checks_pass else "rejected",
        )
        if not risk.all_checks_pass:
            logger.info("Skipping %s: risk gate blocked trade (%s)", signal.mint_address, ", ".join(risk.reasons))
            self._log_rejection_record(rejection_record)
            return DecisionResult(
                trade=None,
                rejection_reason=self._risk_rejection_reason(risk),
                metadata={**regime_metadata, "rejection_record": asdict(rejection_record)},
            )

        open_positions = await self.position_manager.get_all_open()
        if len(open_positions) >= self.config.position.max_open_positions:
            logger.info("Skipping %s: max open positions reached", signal.mint_address)
            self._log_rejection_record(rejection_record)
            return DecisionResult(
                trade=None,
                rejection_reason="max_open_positions_reached",
                metadata={**regime_metadata, "rejection_record": asdict(rejection_record)},
            )

        remaining_portfolio = (
            self.config.position.max_portfolio_sol - await self.position_manager.total_exposure_sol()
        )
        if remaining_portfolio <= 0:
            logger.info("Skipping %s: max portfolio exposure reached", signal.mint_address)
            self._log_rejection_record(rejection_record)
            return DecisionResult(
                trade=None,
                rejection_reason="max_portfolio_exposure_reached",
                metadata={**regime_metadata, "rejection_record": asdict(rejection_record)},
            )

        sizing = self._calculate_position_size(signal, risk, remaining_portfolio)
        if sizing.amount_sol <= 0:
            logger.info("Skipping %s: calculated position size was zero", signal.mint_address)
            self._log_rejection_record(rejection_record)
            return DecisionResult(
                trade=None,
                rejection_reason=sizing.rejection_reason or "position_size_zero",
                metadata={**sizing.metadata, **regime_metadata, "rejection_record": asdict(rejection_record)},
            )

        trade = await self.execution.execute_swap(
            signal.mint_address,
            Side.BUY,
            sizing.amount_sol,
            slippage_bps=self.config.position.default_slippage_bps,
        )
        trade = trade.model_copy(
            update={
                "mode": self.execution.mode,
                "metadata": {
                    **trade.metadata,
                    **sizing.metadata,
                    **regime_metadata,
                    "signal_source": signal.source.value,
                    "signal_type": signal.type.value,
                    "signal_confidence": signal.confidence,
                    "signal_weight": signal.weight,
                    "risk_score": risk.score,
                    "risk_reasons": risk.reasons,
                    "attention_score": _signal_attention_value(signal.payload, "attention_score"),
                    "attention_tier": _signal_attention_value(signal.payload, "attention_tier"),
                    "attention_reasons": _signal_attention_value(signal.payload, "attention_reasons"),
                    "narrative_tags": _signal_attention_value(signal.payload, "narrative_tags"),
                    "social_signal_state": _signal_attention_value(signal.payload, "social_signal_state"),
                    "metadata_completeness_state": _signal_attention_value(signal.payload, "metadata_completeness_state"),
                },
            }
        )
        await self._record_trade(trade)
        await self.position_manager.open_position(trade, signal)
        self._log_rejection_record(rejection_record)
        return DecisionResult(
            trade=trade,
            metadata={**sizing.metadata, **regime_metadata, "rejection_record": asdict(rejection_record)},
        )

    async def check_exits(self) -> list[Trade]:
        exit_trades: list[Trade] = []
        for position in await self.position_manager.get_all_open():
            current_price = await self.execution.get_current_price(position.mint_address)
            if current_price is None or current_price <= 0:
                logger.info("Skipping exit evaluation for %s: no current price", position.mint_address)
                continue

            refreshed_risk = await self._reassess_position(position.mint_address)
            liquidity_sol = refreshed_risk.token.liquidity_sol if refreshed_risk.token else None
            actions = evaluate_exits(
                position,
                current_price,
                liquidity_sol,
                self.config,
                risk_assessment=refreshed_risk,
            )
            if not actions:
                continue

            remaining_pct = position.remaining_sell_pct
            sell_pct = min(sum(action.sell_pct for action in actions), remaining_pct)
            if sell_pct <= 0:
                continue

            trade = await self.execution.execute_swap(
                position.mint_address,
                Side.SELL,
                max(position.amount_sol * sell_pct, 0.000001),
                slippage_bps=self.config.position.default_slippage_bps,
            )
            trade = trade.model_copy(
                update={
                    "mode": self.execution.mode,
                    "metadata": {
                        **trade.metadata,
                        "exit_reasons": [action.reason for action in actions],
                        "sell_pct": sell_pct,
                    },
                }
            )
            await self._record_trade(trade)

            realized_pnl = self._realized_pnl(position, trade.price_sol or current_price, sell_pct)
            exit_marker = self._build_exit_marker(position, trade, sell_pct)
            await self.position_manager.record_partial_exit(
                position.mint_address,
                exit_marker,
                realized_pnl_sol=realized_pnl,
            )

            if any(action.is_full_exit for action in actions) or sell_pct >= remaining_pct:
                await self.position_manager.close_position(position.mint_address)

            exit_trades.append(trade)

        return exit_trades

    def _calculate_position_size(
        self,
        signal: Signal,
        risk: RiskAssessment,
        remaining_portfolio: float,
    ) -> PositionSizingResult:
        strength = max(0.0, min(signal.confidence * max(signal.weight, 0.0), 1.0))
        capped_single = min(
            self.config.position.max_single_position_sol,
            remaining_portfolio,
        )
        flat_size = round(min(capped_single * strength, self.config.position.max_single_position_sol), 6)
        token = risk.token or self._coerce_token(signal.payload.get("token"), signal.mint_address)
        liquidity_sol = token.liquidity_sol

        metadata: dict[str, object] = {
            "position_sizing_mode": "flat",
            "position_sizing_enabled": self.config.position.liquidity_sizing_enabled,
            "position_sizing_requested_sol": flat_size,
            "position_sizing_max_position_cap_sol": round(capped_single, 6),
            "position_sizing_reason": "liquidity_sizing_disabled",
            "position_sizing_skip_trade": False,
            "position_sizing_capped": False,
        }
        if liquidity_sol is not None:
            metadata["position_sizing_liquidity_sol"] = round(liquidity_sol, 6)

        if not self.config.position.liquidity_sizing_enabled:
            return PositionSizingResult(amount_sol=flat_size, metadata=metadata)

        liquidity_decision = determine_liquidity_position_size(liquidity_sol)
        effective_cap = round(min(liquidity_decision.max_position_sol, capped_single), 6)
        skip_trade = liquidity_decision.skip_trade or effective_cap <= 0
        final_size = 0.0 if skip_trade else round(min(flat_size, effective_cap), 6)
        metadata.update(
            {
                "position_sizing_mode": "liquidity",
                "position_sizing_helper_max_position_sol": liquidity_decision.max_position_sol,
                "position_sizing_max_position_cap_sol": effective_cap,
                "position_sizing_reason": liquidity_decision.reason,
                "position_sizing_skip_trade": skip_trade,
                "position_sizing_capped": skip_trade or final_size < flat_size,
            }
        )

        rejection_reason = None
        if skip_trade:
            rejection_reason = f"liquidity_sizing_{liquidity_decision.reason}"
        return PositionSizingResult(
            amount_sol=final_size,
            metadata=metadata,
            rejection_reason=rejection_reason,
        )

    def _market_regime_metadata(self, signal: Signal) -> dict[str, object]:
        if not self.market_regime_enabled:
            return {}

        regime = detect_market_regime(self._market_regime_inputs(signal))
        return {
            "market_regime_enabled": True,
            "market_regime": regime.regime,
            "market_regime_confidence": regime.confidence,
            "market_regime_reasons": list(regime.reason_labels),
            "market_regime_input_summary": regime.input_summary,
            "market_regime_adjustment_hints": {
                "position_cap_multiplier": regime.adjustment_hints.position_cap_multiplier,
                "signal_threshold_multiplier": regime.adjustment_hints.signal_threshold_multiplier,
                "risk_appetite": regime.adjustment_hints.risk_appetite,
            },
        }

    def _market_regime_inputs(self, signal: Signal) -> MarketRegimeInputs:
        payload = signal.payload
        return MarketRegimeInputs(
            new_pool_count=self._coerce_int(payload.get("new_pool_count") or payload.get("newPoolCount")),
            average_liquidity_sol=self._coerce_float(payload.get("average_liquidity_sol") or payload.get("averageLiquiditySol")),
            median_volume_sol=self._coerce_float(payload.get("median_volume_sol") or payload.get("medianVolumeSol")),
            median_transaction_count=self._coerce_float(payload.get("median_transaction_count") or payload.get("medianTransactionCount")),
            paper_trade_success_rate=self._coerce_float(payload.get("paper_trade_success_rate") or payload.get("paperTradeSuccessRate")),
            paper_trade_sample_size=self._coerce_int(payload.get("paper_trade_sample_size") or payload.get("paperTradeSampleSize")),
            signal_count=self._coerce_int(payload.get("signal_count") or payload.get("signalCount")),
            signal_velocity_per_hour=self._coerce_float(payload.get("signal_velocity_per_hour") or payload.get("signalVelocityPerHour")),
        )

    async def _assess_signal(self, signal: Signal) -> RiskAssessment:
        if isinstance(signal.payload.get("risk_assessment"), RiskAssessment):
            return signal.payload["risk_assessment"]

        token = self._coerce_token(signal.payload.get("token"), signal.mint_address)
        scorer = self.risk_scorer

        for method_name, args in (
            ("assess_signal", (signal,)),
            ("assess", (signal,)),
            ("assess_token", (token, self.config.risk)),
        ):
            method = getattr(scorer, method_name, None)
            if method is None:
                continue
            try:
                result = method(*args)
            except TypeError:
                continue
            return await self._maybe_await(result)

        if callable(scorer):
            for args in ((signal, self.config.risk), (signal,), (token, self.config.risk)):
                try:
                    return await self._maybe_await(scorer(*args))
                except (TypeError, AttributeError):
                    continue

        raise TypeError("risk_scorer must provide assess_signal(), assess(), assess_token(), or be callable")

    async def _reassess_position(self, mint_address: str) -> RiskAssessment:
        token = TokenInfo(mint_address=mint_address)
        scorer = self.risk_scorer
        method = getattr(scorer, "assess_token", None)
        if method is not None:
            try:
                return await self._maybe_await(method(token, self.config.risk))
            except TypeError:
                pass

        if callable(scorer):
            try:
                return await self._maybe_await(scorer(token, self.config.risk))
            except TypeError:
                pass

        return RiskAssessment(token=token)

    async def _record_trade(self, trade: Trade) -> None:
        if self.db is None:
            return
        await record_trade(self.db, trade)

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    @staticmethod
    def _coerce_token(raw_token: object, mint_address: str) -> TokenInfo:
        if isinstance(raw_token, TokenInfo):
            return raw_token
        if isinstance(raw_token, dict):
            return TokenInfo.model_validate({"mint_address": mint_address, **raw_token})
        return TokenInfo(mint_address=mint_address)

    @staticmethod
    def _risk_rejection_reason(risk: RiskAssessment) -> str:
        for field_name in (
            "liquidity_check",
            "top10_holder_check",
            "creator_holding_check",
            "age_check",
            "unique_buyers_check",
            "mint_authority_check",
            "freeze_authority_check",
            "honeypot_check",
        ):
            result = getattr(risk, field_name)
            if result == CheckResult.FAIL:
                return f"{field_name}_failed"
        for field_name in (
            "liquidity_check",
            "top10_holder_check",
            "creator_holding_check",
            "age_check",
            "unique_buyers_check",
            "mint_authority_check",
            "freeze_authority_check",
            "honeypot_check",
        ):
            result = getattr(risk, field_name)
            if result == CheckResult.UNKNOWN:
                return f"{field_name}_unknown"
        return "risk_check_blocked"

    def _build_rejection_record(
        self,
        signal: Signal,
        *,
        risk: RiskAssessment | None = None,
        outcome: str,
    ) -> RejectionRecord:
        check_results: dict[str, dict[str, object]] = {}
        failed_check: str | None = None
        token = risk.token if risk is not None else None
        for check_name, actual, threshold, result in self._ordered_check_details(token, risk):
            check_results[check_name] = {
                "passed": result == CheckResult.PASS,
                "result": result.value,
                "value": actual,
                "threshold": threshold,
            }
            if failed_check is None and result == CheckResult.FAIL:
                failed_check = check_name

        timestamp = risk.checked_at if risk is not None else signal.observed_at
        return RejectionRecord(
            mint_address=signal.mint_address,
            signal_source=signal.source.value,
            signal_strength=round(max(0.0, min(signal.confidence * max(signal.weight, 0.0), 1.0)), 6),
            timestamp=timestamp,
            outcome=outcome,
            failed_check=failed_check,
            risk_score=risk.score if risk is not None else None,
            check_results=check_results,
        )

    def _ordered_check_details(
        self,
        token: TokenInfo | None,
        risk: RiskAssessment | None,
    ) -> tuple[tuple[str, object, object, CheckResult], ...]:
        risk = risk or RiskAssessment(token=token)
        return (
            ("liquidity_check", token.liquidity_sol if token else None, self.config.risk.min_liquidity_sol, risk.liquidity_check),
            ("top10_holder_check", token.top10_holder_pct if token else None, self.config.risk.max_top10_holder_pct, risk.top10_holder_check),
            (
                "creator_holding_check",
                token.creator_holding_pct if token else None,
                self.config.risk.max_creator_holding_pct,
                risk.creator_holding_check,
            ),
            ("age_check", token.age_minutes if token else None, self.config.risk.min_age_minutes, risk.age_check),
            ("unique_buyers_check", token.unique_buyers if token else None, self.config.risk.min_unique_buyers, risk.unique_buyers_check),
            (
                "mint_authority_check",
                token.mint_authority_revoked if token else None,
                True,
                risk.mint_authority_check,
            ),
            (
                "freeze_authority_check",
                token.freeze_authority_revoked if token else None,
                True,
                risk.freeze_authority_check,
            ),
            ("honeypot_check", None, False, risk.honeypot_check),
        )

    @staticmethod
    def _log_rejection_record(record: RejectionRecord) -> None:
        payload = asdict(record)
        if structlog is not None:
            telemetry_logger.info("decision_evaluation", rejection_record=payload)
            return
        telemetry_logger.info("decision_evaluation %s", json.dumps(payload, default=str, sort_keys=True))

    @staticmethod
    def summarize_rejections(records: list[RejectionRecord]) -> dict[str, object]:
        rejection_reasons = Counter(
            record.failed_check or "other" for record in records if record.outcome == "rejected"
        )
        source_distribution = Counter(record.signal_source for record in records)
        passed_count = sum(1 for record in records if record.outcome == "passed")
        rejected_count = sum(1 for record in records if record.outcome == "rejected")
        return {
            "total_evaluated": len(records),
            "passed_count": passed_count,
            "rejected_count": rejected_count,
            "rejection_reason_distribution": rejection_reasons,
            "source_distribution": source_distribution,
        }

    @staticmethod
    def _realized_pnl(position: Position, exit_price_sol: float, sell_pct: float) -> float:
        token_amount = position.token_amount * sell_pct
        return round(token_amount * (exit_price_sol - position.entry_price_sol), 9)

    @staticmethod
    def _build_exit_marker(position: Position, trade: Trade, sell_pct: float) -> PartialExit:
        multiple = (trade.price_sol or position.entry_price_sol) / position.entry_price_sol
        return PartialExit(
            multiple=max(multiple, 0.000001),
            sell_pct=sell_pct,
            executed=True,
            trade_id=trade.id,
            executed_at=trade.executed_at,
        )

    @staticmethod
    def _coerce_int(value: object) -> int | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


def _signal_attention_value(payload: dict[str, object], key: str) -> object:
    diagnostics = payload.get("attention_diagnostics")
    if isinstance(diagnostics, dict):
        return diagnostics.get(key)
    return None
