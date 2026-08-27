"""PumpPortal price stream with global feed-health monitoring.

The runtime retains held-token trade marks for high-frequency stop checks. Feed
health is separate: any PumpPortal message refreshes one global timestamp, so
a quiet held token cannot be mistaken for a disconnected provider.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable

import websockets

log = logging.getLogger("pumpportal_price")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

PriceHandler = Callable[[str, float], Awaitable[None]]
MintsProvider = Callable[[], Awaitable[set[str]]]
StaleHandler = Callable[[], Awaitable[None]]


class PumpPortalPriceFeed:
    """Keep held-token marks and global PumpPortal feed health independent."""

    def __init__(
        self,
        held_mints: MintsProvider,
        on_price: PriceHandler,
        on_stale: StaleHandler | None = None,
        *,
        url: str = PUMPPORTAL_WS_URL,
        refresh_interval_s: float = 1.0,
        reconnect_delay_s: float = 2.0,
        stale_after_s: float = 15.0,
    ) -> None:
        self._held_mints = held_mints
        self._on_price = on_price
        self._on_stale = on_stale
        self._url = url
        self._refresh_interval_s = refresh_interval_s
        self._reconnect_delay_s = reconnect_delay_s
        self._stale_after_s = stale_after_s
        self._last_pumpportal_event_at = time.monotonic()
        self._stale_notified = False

    async def run(self) -> None:
        """Reconnect indefinitely while retaining stale detection across reconnects."""
        log.info(
            "PRICE_FEED: PumpPortal WebSocket starting; "
            "global stale feed triggers fail-closed handler",
        )
        while True:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    log.info("PRICE_FEED: PumpPortal WebSocket connected")
                    await self._run_connection(websocket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("PRICE_FEED: PumpPortal disconnected: %s", exc)
                await self._wait_to_reconnect()

    async def _wait_to_reconnect(self) -> None:
        """Keep evaluating stream freshness while reconnect backoff is in progress."""
        deadline = time.monotonic() + self._reconnect_delay_s
        while True:
            await self._notify_stale_feed()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.1, remaining))

    async def _run_connection(self, websocket: object) -> None:
        subscribed: set[str] = set()
        await websocket.send(json.dumps({"method": "subscribeNewToken"}))
        await websocket.send(json.dumps({"method": "subscribeMigration"}))
        while True:
            held = await self._held_mints()
            additions = held - subscribed
            removals = subscribed - held
            if additions:
                await websocket.send(
                    json.dumps({"method": "subscribeTokenTrade", "keys": sorted(additions)}),
                )
                subscribed.update(additions)
            if removals:
                await websocket.send(
                    json.dumps({"method": "unsubscribeTokenTrade", "keys": sorted(removals)}),
                )
                subscribed.difference_update(removals)

            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=self._refresh_interval_s)
            except TimeoutError:
                await self._notify_stale_feed()
                continue
            self._last_pumpportal_event_at = time.monotonic()
            self._stale_notified = False
            price = _parse_price_update(raw)
            if price is not None and price[0] in subscribed:
                await self._on_price(*price)
            await self._notify_stale_feed()

    async def _notify_stale_feed(self) -> None:
        """Fail closed only when PumpPortal has sent no message of any kind."""
        if self._on_stale is None or self._stale_notified:
            return
        now = time.monotonic()
        elapsed = now - self._last_pumpportal_event_at
        if elapsed < self._stale_after_s:
            return
        self._stale_notified = True
        log.warning(
            "PRICE_FEED: global PumpPortal feed stale age=%.1fs; triggering fail-closed handler",
            elapsed,
        )
        try:
            await self._on_stale()
        except Exception as exc:
            log.error("PRICE_FEED: global stale handler failed: %s", exc)


def _parse_price_update(raw: object) -> tuple[str, float] | None:
    """Normalize PumpPortal's token-trade payload without trusting malformed data."""
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    mint = payload.get("mint")
    if not isinstance(mint, str) or not mint:
        return None

    for key in ("priceSol", "price_sol", "price"):
        value = _finite_positive(payload.get(key))
        if value is not None:
            return mint, value

    sol_amount = _finite_positive(payload.get("solAmount"))
    token_amount = _finite_positive(payload.get("tokenAmount"))
    if sol_amount is None or token_amount is None:
        return None
    return mint, sol_amount / token_amount


def _finite_positive(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None
