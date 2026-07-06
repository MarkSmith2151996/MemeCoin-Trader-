"""Alert dispatch helpers with optional external channels."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import httpx

try:
    import structlog
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    structlog = None

from src.core.models import Trade

LOGGER = structlog.get_logger(__name__) if structlog is not None else logging.getLogger(__name__)
ALERT_LEVELS = {"info", "warning", "critical"}


def log_info(event: str, **payload: Any) -> None:
    if structlog is not None:
        LOGGER.info(event, **payload)
        return
    LOGGER.info("%s %s", event, payload)


def log_warning(event: str, **payload: Any) -> None:
    if structlog is not None:
        LOGGER.warning(event, **payload)
        return
    LOGGER.warning("%s %s", event, payload)


@dataclass(slots=True)
class AlertManager:
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    discord_webhook_url: str | None = None
    timeout_s: float = 10.0

    @classmethod
    def from_env(cls) -> "AlertManager":
        return cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL"),
        )

    async def send(self, level: str, title: str, message: str) -> None:
        """Send alert through configured channels. Level: info, warning, critical."""

        normalized_level = level.lower().strip()
        if normalized_level not in ALERT_LEVELS:
            raise ValueError(f"unsupported alert level: {level}")
        if not title.strip():
            raise ValueError("title cannot be empty")
        if not message.strip():
            raise ValueError("message cannot be empty")

        payload = {"level": normalized_level, "title": title.strip(), "message": message.strip()}
        log_info("alert.sent", **payload, channels=self.enabled_channels())

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            if self.telegram_enabled:
                await self._send_telegram(client, payload)
            if self.discord_enabled:
                await self._send_discord(client, payload)

    async def component_stale(self, component: str, age_s: int | None = None) -> None:
        detail = f"Heartbeat missing for {component}"
        if age_s is not None:
            detail = f"Heartbeat missing for {component} ({age_s}s stale)"
        await self.send("critical", "Component stale", detail)

    async def trade_executed(self, trade: Trade) -> None:
        await self.send(
            "info",
            "Trade executed",
            f"{trade.side.value} {trade.amount_sol:.4f} SOL on {trade.mint_address} in {trade.mode} mode",
        )

    async def take_profit_hit(self, mint_address: str, multiple: float) -> None:
        await self.send("info", "Take-profit hit", f"{mint_address} reached {multiple:.2f}x target")

    async def stop_loss_hit(self, mint_address: str, stop_loss_pct: float) -> None:
        await self.send(
            "warning",
            "Stop-loss hit",
            f"{mint_address} hit stop-loss threshold at {stop_loss_pct:.2%}",
        )

    async def risk_check_blocked_trade(self, mint_address: str, reasons: list[str]) -> None:
        message = "; ".join(reasons) if reasons else "Risk checks blocked the trade"
        await self.send("warning", "Trade blocked", f"{mint_address}: {message}")

    async def live_mode_detected(self, mode: str) -> None:
        await self.send("critical", "Live mode detected", f"Execution mode is {mode}")

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def discord_enabled(self) -> bool:
        return bool(self.discord_webhook_url)

    def enabled_channels(self) -> list[str]:
        channels = ["log"]
        if self.telegram_enabled:
            channels.append("telegram")
        if self.discord_enabled:
            channels.append("discord")
        return channels

    async def _send_telegram(self, client: httpx.AsyncClient, payload: dict[str, Any]) -> None:
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        body = {
            "chat_id": self.telegram_chat_id,
            "text": format_alert_message(payload),
            "disable_web_page_preview": True,
        }
        try:
            response = await client.post(url, json=body)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            log_warning("alert.telegram_failed", error=str(exc), title=payload["title"])

    async def _send_discord(self, client: httpx.AsyncClient, payload: dict[str, Any]) -> None:
        body = {"content": format_alert_message(payload)}
        try:
            response = await client.post(str(self.discord_webhook_url), json=body)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            log_warning("alert.discord_failed", error=str(exc), title=payload["title"])


def format_alert_message(payload: dict[str, Any]) -> str:
    return f"[{payload['level'].upper()}] {payload['title']}\n{payload['message']}"


async def send_alert(message: str) -> None:
    if not message:
        raise ValueError("message cannot be empty")
    await AlertManager.from_env().send("info", "Alert", message)
