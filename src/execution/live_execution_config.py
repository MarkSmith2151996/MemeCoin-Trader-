"""Conservative live execution config validation and RPC selection."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping
from urllib.parse import urlparse

from src.core.config import Settings


@dataclass(frozen=True, slots=True)
class LiveExecutionConfigDecision:
    allowed: bool
    diagnostics: tuple[str, ...]
    priority_fee_lamports: int | None
    jito_tip_lamports: int | None
    primary_rpc_label: str | None
    backup_rpc_label: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "diagnostics": list(self.diagnostics),
            "priority_fee_lamports": self.priority_fee_lamports,
            "jito_tip_lamports": self.jito_tip_lamports,
            "primary_rpc_label": self.primary_rpc_label,
            "backup_rpc_label": self.backup_rpc_label,
        }


def evaluate_live_execution_config(
    settings: Settings,
    *,
    env: Mapping[str, str] | None = None,
) -> LiveExecutionConfigDecision:
    resolved_env = env if env is not None else os.environ
    diagnostics: list[str] = []

    priority_fee_lamports = _env_int(
        resolved_env,
        "PRIORITY_FEE_LAMPORTS",
        settings.execution.priority_fee_lamports,
    )
    jito_tip_lamports = _env_int(
        resolved_env,
        "JITO_TIP_LAMPORTS",
        settings.execution.jito_tip_lamports,
    )
    primary_rpc_url = _env_str(
        resolved_env,
        "PRIMARY_RPC_URL",
        settings.execution.primary_rpc_url,
    )
    backup_rpc_url = _env_str(
        resolved_env,
        "BACKUP_RPC_URL",
        settings.execution.backup_rpc_url,
    )

    if priority_fee_lamports is None:
        diagnostics.append("priority_fee_config_invalid")
    elif not settings.execution.min_priority_fee_lamports <= priority_fee_lamports <= settings.execution.max_priority_fee_lamports:
        diagnostics.append("priority_fee_out_of_bounds")

    if jito_tip_lamports is None:
        diagnostics.append("jito_tip_config_invalid")
    elif not 0 <= jito_tip_lamports <= settings.execution.max_jito_tip_lamports:
        diagnostics.append("jito_tip_out_of_bounds")

    if settings.execution.mode == "live" and not primary_rpc_url:
        diagnostics.append("primary_rpc_url_missing")

    if diagnostics:
        return LiveExecutionConfigDecision(
            allowed=False,
            diagnostics=tuple(diagnostics),
            priority_fee_lamports=priority_fee_lamports,
            jito_tip_lamports=jito_tip_lamports,
            primary_rpc_label=_rpc_label(primary_rpc_url),
            backup_rpc_label=_rpc_label(backup_rpc_url),
        )

    return LiveExecutionConfigDecision(
        allowed=True,
        diagnostics=("live_execution_config_valid",),
        priority_fee_lamports=priority_fee_lamports,
        jito_tip_lamports=jito_tip_lamports,
        primary_rpc_label=_rpc_label(primary_rpc_url),
        backup_rpc_label=_rpc_label(backup_rpc_url),
    )


def _env_int(env: Mapping[str, str], name: str, default: int | None) -> int | None:
    raw_value = env.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return int(raw_value)
    except ValueError:
        return None


def _env_str(env: Mapping[str, str], name: str, default: str | None) -> str | None:
    raw_value = env.get(name)
    if raw_value is None:
        return default
    stripped = raw_value.strip()
    return stripped or None


def _rpc_label(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.netloc:
        return parsed.netloc
    return "configured"
