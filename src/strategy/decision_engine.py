"""Risk-gated trading decisions and exit execution."""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import Any

from src.core.config import Settings
from src.core.database import record_trade
from src.core.models import PartialExit, Position, RiskAssessment, Side, Signal, TokenInfo, Trade
from src.execution.base import ExecutionAdapter
from src.strategy.exits import evaluate_exits
from src.strategy.position_manager import PositionManager


logger = logging.getLogger(__name__)


class DecisionEngine:
    def __init__(
        self,
        execution: ExecutionAdapter,
        risk_scorer: Any,
        position_manager: PositionManager,
        config: Settings,
        db: str | Path | None = None,
    ) -> None:
        self.execution = execution
        self.risk_scorer = risk_scorer
        self.position_manager = position_manager
        self.config = config
        self.db = Path(db) if db is not None else None

    async def evaluate_signal(self, signal: Signal) -> Trade | None:
        if not signal.mint_address.strip():
            logger.info("Skipping signal without mint address")
            return None

        existing = await self.position_manager.get_position(signal.mint_address)
        if existing is not None:
            logger.info("Skipping %s: open position already exists", signal.mint_address)
            return None

        risk = await self._assess_signal(signal)
        if not risk.all_checks_pass:
            logger.info("Skipping %s: risk gate blocked trade (%s)", signal.mint_address, ", ".join(risk.reasons))
            return None

        open_positions = await self.position_manager.get_all_open()
        if len(open_positions) >= self.config.position.max_open_positions:
            logger.info("Skipping %s: max open positions reached", signal.mint_address)
            return None

        remaining_portfolio = (
            self.config.position.max_portfolio_sol - await self.position_manager.total_exposure_sol()
        )
        if remaining_portfolio <= 0:
            logger.info("Skipping %s: max portfolio exposure reached", signal.mint_address)
            return None

        position_size = self._calculate_position_size(signal, remaining_portfolio)
        if position_size <= 0:
            logger.info("Skipping %s: calculated position size was zero", signal.mint_address)
            return None

        trade = await self.execution.execute_swap(
            signal.mint_address,
            Side.BUY,
            position_size,
            slippage_bps=self.config.position.default_slippage_bps,
        )
        trade = trade.model_copy(
            update={
                "mode": self.execution.mode,
                "metadata": {
                    **trade.metadata,
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
        return trade

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

    def _calculate_position_size(self, signal: Signal, remaining_portfolio: float) -> float:
        strength = max(0.0, min(signal.confidence * max(signal.weight, 0.0), 1.0))
        capped_single = min(
            self.config.position.max_single_position_sol,
            remaining_portfolio,
        )
        size = capped_single * strength
        return round(min(size, self.config.position.max_single_position_sol), 6)

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
            try:
                return await self._maybe_await(scorer(signal))
            except (TypeError, AttributeError):
                return await self._maybe_await(scorer(token, self.config.risk))

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
