"""Risk-gated trading decisions and exit execution."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.config import Settings
from src.core.database import record_trade
from src.core.models import CheckResult, PartialExit, Position, RiskAssessment, Side, Signal, TokenInfo, Trade
from src.execution.base import ExecutionAdapter
from src.strategy.exits import evaluate_exits
from src.strategy.market_regime import MarketRegimeInputs, detect_market_regime
from src.strategy.position_manager import PositionManager
from src.strategy.position_sizing import determine_liquidity_position_size


logger = logging.getLogger(__name__)


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
            return DecisionResult(trade=None, rejection_reason="missing_mint_address")

        existing = await self.position_manager.get_position(signal.mint_address)
        if existing is not None:
            logger.info("Skipping %s: open position already exists", signal.mint_address)
            return DecisionResult(trade=None, rejection_reason="open_position_exists")

        regime_metadata = self._market_regime_metadata(signal)

        risk = await self._assess_signal(signal)
        if not risk.all_checks_pass:
            logger.info("Skipping %s: risk gate blocked trade (%s)", signal.mint_address, ", ".join(risk.reasons))
            return DecisionResult(
                trade=None,
                rejection_reason=self._risk_rejection_reason(risk),
                metadata=regime_metadata,
            )

        open_positions = await self.position_manager.get_all_open()
        if len(open_positions) >= self.config.position.max_open_positions:
            logger.info("Skipping %s: max open positions reached", signal.mint_address)
            return DecisionResult(
                trade=None,
                rejection_reason="max_open_positions_reached",
                metadata=regime_metadata,
            )

        remaining_portfolio = (
            self.config.position.max_portfolio_sol - await self.position_manager.total_exposure_sol()
        )
        if remaining_portfolio <= 0:
            logger.info("Skipping %s: max portfolio exposure reached", signal.mint_address)
            return DecisionResult(
                trade=None,
                rejection_reason="max_portfolio_exposure_reached",
                metadata=regime_metadata,
            )

        sizing = self._calculate_position_size(signal, risk, remaining_portfolio)
        if sizing.amount_sol <= 0:
            logger.info("Skipping %s: calculated position size was zero", signal.mint_address)
            return DecisionResult(
                trade=None,
                rejection_reason=sizing.rejection_reason or "position_size_zero",
                metadata={**sizing.metadata, **regime_metadata},
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
                },
            }
        )
        await self._record_trade(trade)
        await self.position_manager.open_position(trade, signal)
        return DecisionResult(trade=trade, metadata={**sizing.metadata, **regime_metadata})

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
