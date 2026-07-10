"""Shared Pydantic models for signals, risk, trades, and positions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(UTC)


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class SignalSource(StrEnum):
    TWITTER = "TWITTER"
    WHALE_TRACKER = "WHALE_TRACKER"
    ONCHAIN = "ONCHAIN"
    PUMP_FUN = "PUMP_FUN"
    MANUAL = "MANUAL"


class SignalType(StrEnum):
    MENTION = "MENTION"
    BUY = "BUY"
    NEW_POOL = "NEW_POOL"
    VOLUME_SPIKE = "VOLUME_SPIKE"
    GRADUATION = "GRADUATION"


class PositionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PARTIAL = "PARTIAL"


class CheckResult(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class TokenInfo(BaseModel):
    """Known facts about a Solana token mint."""

    model_config = ConfigDict(extra="forbid")

    mint_address: str
    symbol: str | None = None
    name: str | None = None
    decimals: int = Field(default=9, ge=0, le=18)
    supply: float | None = Field(default=None, ge=0)
    creator_address: str | None = None
    created_at: datetime | None = None
    liquidity_sol: float | None = Field(default=None, ge=0)
    market_cap_usd: float | None = Field(default=None, ge=0)
    holder_count: int | None = Field(default=None, ge=0)
    unique_buyers: int | None = Field(default=None, ge=0)
    top10_holder_pct: float | None = Field(default=None, ge=0, le=100)
    creator_holding_pct: float | None = Field(default=None, ge=0, le=100)
    mint_authority_revoked: bool | None = None
    freeze_authority_revoked: bool | None = None

    @property
    def age_minutes(self) -> float | None:
        if self.created_at is None:
            return None
        return max((utc_now() - self.created_at).total_seconds() / 60, 0.0)


class RiskAssessment(BaseModel):
    """Risk check outcomes for a token."""

    model_config = ConfigDict(extra="forbid")

    token: TokenInfo | None = None
    liquidity_check: CheckResult = CheckResult.UNKNOWN
    top10_holder_check: CheckResult = CheckResult.UNKNOWN
    creator_holding_check: CheckResult = CheckResult.UNKNOWN
    age_check: CheckResult = CheckResult.UNKNOWN
    unique_buyers_check: CheckResult = CheckResult.UNKNOWN
    mint_authority_check: CheckResult = CheckResult.UNKNOWN
    freeze_authority_check: CheckResult = CheckResult.UNKNOWN
    honeypot_check: CheckResult = CheckResult.UNKNOWN
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    reasons: list[str] = Field(default_factory=list)
    checked_at: datetime = Field(default_factory=utc_now)

    @property
    def all_checks_pass(self) -> bool:
        checks = [
            self.liquidity_check,
            self.top10_holder_check,
            self.creator_holding_check,
            self.age_check,
            self.unique_buyers_check,
            self.mint_authority_check,
            self.freeze_authority_check,
            self.honeypot_check,
        ]
        return all(result == CheckResult.PASS for result in checks)


class Signal(BaseModel):
    """A normalized opportunity signal from one upstream source."""

    model_config = ConfigDict(extra="forbid")

    source: SignalSource
    type: SignalType
    mint_address: str
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    weight: float = Field(default=1.0, ge=0.0)
    message: str | None = None
    payload: dict[str, object] = Field(default_factory=dict)
    observed_at: datetime = Field(default_factory=utc_now)


class Trade(BaseModel):
    """Executed or simulated swap record."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid4()))
    mint_address: str
    side: Side
    amount_sol: float = Field(gt=0)
    token_amount: float | None = Field(default=None, ge=0)
    price_sol: float | None = Field(default=None, ge=0)
    slippage_bps: int = Field(default=300, ge=0)
    tx_signature: str | None = None
    mode: str = "paper"
    status: str = "simulated"
    executed_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, object] = Field(default_factory=dict)


class PartialExit(BaseModel):
    """One configured or completed partial exit for a position."""

    model_config = ConfigDict(extra="forbid")
    multiple: float = Field(gt=0)
    sell_pct: float = Field(gt=0, le=1)
    executed: bool = False
    trade_id: str | None = None
    executed_at: datetime | None = None


class Position(BaseModel):
    """Open or closed token position."""

    model_config = ConfigDict(extra="forbid")
    id: str = Field(default_factory=lambda: str(uuid4()))
    mint_address: str
    entry_trade_id: str
    amount_sol: float = Field(gt=0)
    token_amount: float = Field(ge=0)
    entry_price_sol: float = Field(ge=0)
    status: PositionStatus = PositionStatus.OPEN
    mode: str = "paper"
    opened_at: datetime = Field(default_factory=utc_now)
    closed_at: datetime | None = None
    realized_pnl_sol: float = 0.0
    close_price_sol: float | None = None
    partial_exits: list[PartialExit] = Field(default_factory=list)

    @property
    def remaining_sell_pct(self) -> float:
        sold_pct = sum(exit.sell_pct for exit in self.partial_exits if exit.executed)
        return max(1.0 - sold_pct, 0.0)


class SwapQuote(BaseModel):
    """Execution quote for a potential swap."""

    model_config = ConfigDict(extra="forbid")
    mint_address: str
    side: Side
    amount_sol: float = Field(gt=0)
    estimated_out_amount: float = Field(ge=0)
    price_sol: float | None = Field(default=None, ge=0)
    price_impact_pct: float = Field(default=0.0, ge=0)
    slippage_bps: int = Field(default=300, ge=0)
    provider: str = "paper"
    expires_at: datetime | None = None

    @field_validator("expires_at")
    @classmethod
    def expires_at_must_be_future(cls, value: datetime | None) -> datetime | None:
        if value is not None and value <= utc_now() - timedelta(seconds=1):
            raise ValueError("expires_at must be in the future")
        return value
