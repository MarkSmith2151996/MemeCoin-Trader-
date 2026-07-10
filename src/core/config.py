"""Settings loader for YAML defaults and environment overrides."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min_liquidity_sol: float = 10.0
    max_top10_holder_pct: float = 50.0
    max_creator_holding_pct: float = 10.0
    min_age_minutes: int = 5
    min_unique_buyers: int = 20
    require_mint_authority_revoked: bool = True
    require_freeze_authority_revoked: bool = True


class PositionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_single_position_sol: float = 0.5
    max_portfolio_sol: float = 5.0
    max_open_positions: int = 5
    liquidity_sizing_enabled: bool = False
    default_slippage_bps: int = 300
    max_slippage_bps: int = 500


class TakeProfitLevel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    multiple: float = Field(gt=0)
    sell_pct: float = Field(gt=0, le=1)


class ExitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tp_levels: list[TakeProfitLevel] = Field(default_factory=list)
    stop_loss_pct: float = 0.50
    time_stop_minutes: int = 120
    trail_stop_pct: float = 0.30
    dynamic_exits_enabled: bool = False
    dynamic_volume_decay_ratio: float = Field(default=0.20, gt=0.0, le=1.0)
    dynamic_volume_decay_minutes: int = Field(default=15, ge=1)
    dynamic_liquidity_drop_ratio: float = Field(default=0.50, gt=0.0, le=1.0)
    dynamic_liquidity_drop_window_seconds: int = Field(default=60, ge=1)
    dynamic_trail_start_multiple: float | None = Field(default=None, gt=0)


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: str = "paper"
    rpc_provider: str = "helius"
    primary_rpc_url: str | None = None
    backup_rpc_url: str | None = None
    priority_fee_lamports: int = 10_000
    min_priority_fee_lamports: int = 0
    max_priority_fee_lamports: int = 100_000
    jito_tip_lamports: int = 0
    max_jito_tip_lamports: int = 100_000
    tx_retry_count: int = 3
    tx_confirm_timeout_s: int = 30


class LiveGuardrailsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    confirmation_phrase: str = "I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL"
    max_trade_sol: float = 0.01
    max_daily_trades: int = 3
    max_daily_loss_sol: float = 0.05
    min_wallet_balance_sol: float = 0.05


class MonitoringConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    heartbeat_interval_s: int = 60
    log_level: str = "INFO"


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    risk: RiskConfig = Field(default_factory=RiskConfig)
    position: PositionConfig = Field(default_factory=PositionConfig)
    exits: ExitConfig = Field(default_factory=ExitConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    live_guardrails: LiveGuardrailsConfig = Field(default_factory=LiveGuardrailsConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


def load_settings(path: str | Path = "config/settings.yaml") -> Settings:
    settings_path = Path(path)
    data: dict[str, object] = {}
    if settings_path.exists():
        data = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}

    settings = Settings.model_validate(data)
    env_overrides: dict[str, str] = {
        "MAX_POSITION_SOL": "max_single_position_sol",
        "MAX_PORTFOLIO_SOL": "max_portfolio_sol",
        "MAX_SLIPPAGE_BPS": "max_slippage_bps",
    }
    position_updates = settings.position.model_dump()
    for env_name, field_name in env_overrides.items():
        if os.getenv(env_name):
            value: float | int = float(os.environ[env_name])
            if field_name.endswith("bps"):
                value = int(value)
            position_updates[field_name] = value

    live_guardrails_updates = settings.live_guardrails.model_dump()
    live_guardrail_env_overrides: dict[str, str] = {
        "MAX_LIVE_TRADE_SOL": "max_trade_sol",
        "MAX_LIVE_DAILY_TRADES": "max_daily_trades",
        "MAX_LIVE_DAILY_LOSS_SOL": "max_daily_loss_sol",
        "MIN_LIVE_WALLET_BALANCE_SOL": "min_wallet_balance_sol",
    }
    for env_name, field_name in live_guardrail_env_overrides.items():
        if os.getenv(env_name):
            value: float | int = float(os.environ[env_name])
            if field_name.endswith("trades"):
                value = int(value)
            live_guardrails_updates[field_name] = value

    return settings.model_copy(
        update={
            "position": PositionConfig.model_validate(position_updates),
            "live_guardrails": LiveGuardrailsConfig.model_validate(live_guardrails_updates),
        }
    )
