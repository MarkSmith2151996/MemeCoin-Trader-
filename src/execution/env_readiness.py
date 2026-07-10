"""Safe env/secrets visibility diagnostics.

Reports only present/missing for required env var names.
Never prints secret values, partial values, prefixes, or suffixes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


ENV_NAMES: tuple[str, ...] = (
    "HELIUS_API_KEY",
    "TRADING_WALLET_PUBLIC_KEY",
    "TRADING_WALLET_PRIVATE_KEY",
    "LIVE_TRADING_ENABLED",
    "LIVE_CONFIRMATION_PHRASE",
    "LIVE_KILL_SWITCH",
    "MAX_LIVE_TRADE_SOL",
    "MAX_DAILY_LIVE_TRADES",
    "MAX_DAILY_LOSS_SOL",
    "PRIMARY_RPC_URL",
    "BACKUP_RPC_URL",
)


def _resolve_dotenv() -> dict[str, str | None]:
    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        return dict(dotenv_values(dotenv_path))
    return {}


@dataclass(frozen=True, slots=True)
class EnvReadinessItem:
    name: str
    present: bool
    source: str = "os.environ"


@dataclass(frozen=True, slots=True)
class EnvReadinessReport:
    items: tuple[EnvReadinessItem, ...]

    def all_present(self) -> bool:
        return all(item.present for item in self.items)

    def lines(self) -> list[str]:
        lines = [f"env_readiness_ready={'YES' if self.all_present() else 'NO'}"]
        for item in self.items:
            status = "present" if item.present else "MISSING"
            lines.append(f"  {item.name}={status}")
        return lines


def evaluate_env_readiness(
    env: dict[str, str] | None = None,
    *,
    dotenv_path: str | Path | None = None,
) -> EnvReadinessReport:
    resolved = env if env is not None else os.environ
    if env is not None:
        dotenv_data: dict[str, str | None] = {}
    else:
        dotenv_data_actual = _resolve_dotenv() if dotenv_path is None else dict(dotenv_values(dotenv_path))
        dotenv_data = dict(dotenv_data_actual)
    items: list[EnvReadinessItem] = []
    for name in ENV_NAMES:
        raw = resolved.get(name) or dotenv_data.get(name)
        present = bool(raw and str(raw).strip())
        source = "os.environ"
        if dotenv_data and (name not in resolved or not str(resolved.get(name) or "").strip()):
            if name in dotenv_data and bool(str(dotenv_data[name] or "").strip()):
                source = ".env"
        items.append(EnvReadinessItem(name=name, present=present, source=source))
    return EnvReadinessReport(items=tuple(items))
